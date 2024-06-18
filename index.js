const { MongoClient } = require('mongodb')
const axios = require('axios')
const fs = require('fs').promises

// Connection URL and database/collection names
const url = 'mongodb://localhost:27017'
const dbName = 'rd1'
const collectionName = '3271'

// REST API endpoint
const apiEndpoint = 'http://localhost:8000/booking-box/update-bookings'
// Delay interval in milliseconds
const delayInterval = 100

// Function to create a MongoDB client
const createClient = () => new MongoClient(url)

// Function to connect to the database
const connectToDatabase = async (client) => {
  await client.connect()
  console.log('Connected successfully to MongoDB server')
  return client.db(dbName)
}

// Function to read documents with the 'token' property
const readDocumentsWithToken = async (db) => {
  const collection = db.collection(collectionName)
  const query = { token: { $exists: true } }
  const documents = await collection.find(query).toArray()
  return documents
}

// Function to send a single document via REST API
const sendDocument = async (document) => {
  try {
    const response = await axios.post(apiEndpoint, document)
    console.log('Document sent successfully:', response.data)
    return { success: true, document }
  } catch (error) {
    console.error('Error sending document:', error.message)
    return { success: false, document }
  }
}

// Function to add a delay
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

// Function to send documents with delay and track success/failure
const sendDocumentsWithDelay = async (documents) => {
  let successCount = 0
  let failureCount = 0
  const successDocuments = []
  const failDocuments = []
  let sentCount = 0
  const docLen = documents.length

  for (const document of documents) {
    delete document._id // Remove the _id property
    const result = await sendDocument(document)
    if (result.success) {
      successCount += 1
      successDocuments.push(result.document)
    } else {
      failureCount += 1
      failDocuments.push(result.document)
    }
    sentCount += 1
    console.log(`Sent ${sentCount} of ${docLen} documents`)
    await delay(delayInterval)
  }

  logResults(successCount, failureCount, failDocuments)
}

// Function to log results and write failed documents to a JSON file
const logResults = async (successCount, failureCount, failDocuments) => {
  console.log(`Success count: ${successCount}`)
  console.log(`Failure count: ${failureCount}`)

  if (failDocuments.length > 0) {
    await fs.writeFile('failed_documents.json', JSON.stringify(failDocuments, null, 2))
    console.log('Failed documents written to failed_documents.json')
  }
}

// Main function to orchestrate the operations
const main = async () => {
  const client = createClient()
  try {
    const db = await connectToDatabase(client)
    const documents = await readDocumentsWithToken(db)
    await client.close()
    console.log('Documents count:', documents.length)
    await sendDocumentsWithDelay(documents)
  } catch (err) {
    console.error(err)
  } finally {
    await client.close()
  }
}

main()
