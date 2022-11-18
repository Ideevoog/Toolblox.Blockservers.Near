import { startStream, types } from 'near-lake-framework';
import { EventHubProducerClient } from "@azure/event-hubs";
const { BlobServiceClient } = require('@azure/storage-blob');
import { promises as fs } from "fs";

var AZURE_STORAGE_CONNECTION_STRING = 
  process.env.AZURE_STORAGE_CONNECTION_STRING;
var AZURE_EVENTHUB_CONNECTION_STRING = 
  process.env.AZURE_EVENTHUB_CONNECTION_STRING;
var WEBJOBS_SHUTDOWN_FILE =
  process.env.WEBJOBS_SHUTDOWN_FILE;

var lastSnapshot = new Date();

const shutDownRequested = async () => !!(await fs.stat(WEBJOBS_SHUTDOWN_FILE).catch(e => false));

const producerClient = new EventHubProducerClient(AZURE_EVENTHUB_CONNECTION_STRING, "invoiceevent");
const blobServiceClient = BlobServiceClient.fromConnectionString(
  AZURE_STORAGE_CONNECTION_STRING
);
const containerClient = blobServiceClient.getContainerClient("nearcursor");
const blockBlobClient = containerClient.getBlockBlobClient("cursor");
const blobClient = containerClient.getBlobClient("cursor");

const lakeConfig: types.LakeConfig = {
  s3BucketName: "near-lake-data-testnet",
  s3RegionName: "eu-central-1",
  startBlockHeight: 102550527,
};
var cursor : number = lakeConfig.startBlockHeight;

interface MicroInvoiceDto {
  standard: string,
  version: string,
  event: string,
  data: MicroInvoiceDataDto[],
};

interface MicroInvoiceDataDto{
  id: bigint,
  from: string,
  to: string,
  amount: string,
  article: string,
  currency: string
}

var handleStreamerMessageWithShutDown
  = async (streamerMessage: types.StreamerMessage) => {
    try{
      await handleStreamerMessage(streamerMessage);
    }
    catch(ex)
    {
      console.log("Storing cursor at ", cursor);
      var data = cursor.toString();
      await blockBlobClient.upload(data, data.length);
      console.log("Cursor stored");
      throw ex;
    }
  };

async function handleStreamerMessage(
  streamerMessage: types.StreamerMessage
): Promise<void> {
  if (await shutDownRequested())
  {
    throw "Shut down requested";
  }
  if (lastSnapshot < new Date(new Date().getTime() - 5 * 60000))
  {
    lastSnapshot = new Date();
    await storeCursor();
  }
  const createdOn = new Date(streamerMessage.block.header.timestamp / 1000000)
  cursor = streamerMessage.block.header.height;
  const relevantOutcomes = streamerMessage
    .shards
    .flatMap(shard => shard.receiptExecutionOutcomes)
    .map(outcome => ({
      receipt: {
        id: outcome.receipt.receiptId,
        receiverId: outcome.receipt.receiverId,
      },
      events: outcome.executionOutcome.outcome.logs.map(
        (log: string): MicroInvoiceDto => {
          const [_, probablyEvent] = log.match(/^EVENT_JSON:(.*)$/) ?? []
          try {
            var probableEvent = JSON.parse(probablyEvent);
            return probableEvent;
          } catch (e) {
            return
          }
        }
      )
      .filter(event => event !== undefined)
    }))
    .filter(relevantOutcome =>
      relevantOutcome.events.some(
        event => event.standard === "ARC-1" && event.event === "issue_invoice"
      )
    )

  let output = []
  for (let relevantOutcome of relevantOutcomes
    .flatMap(outcome => outcome.events
      .flatMap(e => e.data)
      .map(eventData =>
        ({
          contract: outcome.receipt.receiverId,
          from: eventData.from,
          to : eventData.to,
          article : eventData.article,
          receiptId : outcome.receipt.id,
          id : eventData.id,
          amount : eventData.amount,
          currency : eventData.currency
        }))))
  {
    output.push(relevantOutcome)
  }
  if (output.length) {
    console.log(`We caught fresh invoices!`)    
    const eventDataBatch = await producerClient.createBatch();
    for (let invoice of output)
    {
      eventDataBatch.tryAdd({ body: invoice });
    }
    await producerClient.sendBatch(eventDataBatch);
  }
}

(async () => {
  
  try{
    const downloadBlockBlobResponse = await blobClient.download();
    lakeConfig.startBlockHeight = Number((await streamToBuffer(downloadBlockBlobResponse.readableStreamBody)).toString());
    console.log("Downloaded cursor with content:", lakeConfig.startBlockHeight);    
  }
  catch(ex)
  {
    if (ex.details.errorCode == "BlobNotFound")
    {
      console.log("No cursor found, using default");
    }else{
      throw ex;
    }
  }
  try{
    await startStream(lakeConfig, handleStreamerMessageWithShutDown);
  }
  catch(ex)
  {
    console.error(ex);
    await storeCursor();
  }
  console.log(`Closing producer`)
  await producerClient.close();
})().catch( e => { console.error(e) });

async function storeCursor() {
  console.log("Storing cursor at ", cursor);
  var data = (cursor - 10).toString();
  await blockBlobClient.upload(data, data.length);
  console.log("Cursor stored");
}

async function streamToBuffer(readableStream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readableStream.on("data", (data) => {
      chunks.push(data instanceof Buffer ? data : Buffer.from(data));
    });
    readableStream.on("end", () => {
      resolve(Buffer.concat(chunks));
    });
    readableStream.on("error", reject);
  });
}
