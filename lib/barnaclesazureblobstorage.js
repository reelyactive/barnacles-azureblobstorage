/**
 * Copyright reelyActive 2020
 * We believe in an open Internet of Things
 */


const { BlobServiceClient,
        StorageSharedKeyCredential } = require('@azure/storage-blob');
const Raddec = require('raddec');


const DEFAULT_AZURE_ACCOUNT = '';
const DEFAULT_AZURE_ACCOUNT_KEY = '';
const DEFAULT_CONTAINER_NAME = 'raddec';
const DEFAULT_PRINT_ERRORS = false;
const DEFAULT_RADDEC_OPTIONS = { includePackets: false };


/**
 * BarnaclesAzureBlobStorage Class
 * Detects events and writes to Azure Blob Storage.
 */
class BarnaclesAzureBlobStorage {

  /**
   * BarnaclesAzureBlobStorage constructor
   * @param {Object} options The options as a JSON object.
   * @constructor
   */
  constructor(options) {
    let self = this;
    options = options || {};

    this.account = options.account || DEFAULT_AZURE_ACCOUNT;
    this.accountKey = options.accountKey || DEFAULT_AZURE_ACCOUNT_KEY;
    this.containerName = options.containerName || DEFAULT_CONTAINER_NAME;
    this.printErrors = options.printErrors || DEFAULT_PRINT_ERRORS;
    this.raddecOptions = options.raddec || DEFAULT_RADDEC_OPTIONS;

    if(options.client) {
      this.client = options.client;
    }
    else {
      let url = 'https://' + this.account + '.blob.core.windows.net';
      this.sharedKeyCredential = new StorageSharedKeyCredential(self.account,
                                                              self.accountKey);
      this.client = new BlobServiceClient(url, self.sharedKeyCredential);
    }
  }

  /**
   * Handle an outbound raddec.
   * @param {Raddec} raddec The outbound raddec.
   */
  handleRaddec(raddec) {
    let self = this;
    let blobName = raddec.timestamp + '-' + raddec.transmitterId + '-' +
                   raddec.transmitterIdType;

    // Create flat raddec as string for Azure Blob Storage
    let blobContent = JSON.stringify(raddec.toFlattened(self.raddecOptions));

    upload(self.client, self.containerName, blobName, blobContent,
           self.printErrors);
  }
}


/**
 * Upload the given blobContent.
 * @param {BlobServiceClient} client The Azure Blob Service client.
 * @param {String} containerName The container name.
 * @param {String} blobName The blob name.
 * @param {String} blobContent The blob content.
 * @param {boolean} printErrors Whether to print errors to the console.
 */
async function upload(client, containerName, blobName, blobContent,
                      printErrors) {
  try {
    let containerClient = client.getContainerClient(containerName);
    let blockBlobClient = containerClient.getBlockBlobClient(blobName);
    let uploadBlobResponse = await blockBlobClient.upload(blobContent,
                                                          blobContent.length);
  }
  catch(err) {
    if(err && printErrors) {
      console.error('barnacles-azureblobstorage error:', err);
    }
  }
}


module.exports = BarnaclesAzureBlobStorage;