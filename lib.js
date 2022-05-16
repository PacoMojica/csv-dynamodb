const AWS = require('aws-sdk')
const fs = require('fs')
const readline = require('readline')
const extract = require('extract-zip')

const BATCH_SIZE = 25
const CONCURRENT_REQUESTS = 40
const s3Params = { Bucket: 'aws-techmentoring-paco', Key: 'asdf.zip' }

const ZIP_NAME = '/tmp/asdf.zip'
const CSV_TARGET_DIR = '/tmp'
const CSV_NAME = '/tmp/asdf.csv'

process.env.AWS_NODEJS_CONNECTION_REUSE_ENABLED = 1

const dynamodb = new AWS.DynamoDB.DocumentClient()
const s3 = new AWS.S3({ apiVersion: '2006-03-01' })

function isNumeric (str) {
  return (
    typeof str === 'string' &&
    !isNaN(str) &&
    !isNaN(parseInt(str))
  )
}

function convertToObject (line) {
  const items = line.split(',')
  return {
    accountId: items[0],
    profileId: items[1],
    accountsSK: items[3],
    name: items[4],
    avatarId: isNumeric(items[5]) ? parseInt(items[5]) : '',
    gender: items[6],
    createdAt: items[7],
    updatedAt: items[8],
    birthdate: items[9],
    age: isNumeric(items[10]) ? parseInt(items[10]) : ''
  }
}

async function saveToDynamoDB (items) {
  const putReqs = items.map(item => ({
    PutRequest: {
      Item: item
    }
  }))

  const req = {
    RequestItems: {
      accounts_profiles_dev: putReqs
    }
  }

  await dynamodb.batchWrite(req).promise()
}

async function processFile () {
  const readStream = fs.createReadStream(CSV_NAME, { encoding: 'utf8' })
  const rl = readline.createInterface({
    input: readStream,
    crlfDelay: Infinity
  })

  let firstLine = true
  let items = []
  let promises = []
  let i = 0

  for await (const line of rl) {
    if (firstLine) {
      firstLine = false
      continue
    }

    items.push(convertToObject(line))

    if (items.length % BATCH_SIZE === 0) {
      promises.push(saveToDynamoDB(items))

      if (promises.length % CONCURRENT_REQUESTS === 0) {
        // console.log('awaiting write requests to DynamoDB')
        await Promise.all(promises)
        promises = []
      }

      items = []
    }

    i++
    if (i % 100000 === 0) {
      console.log('Processed lines:', i)
    }
  }

  if (items.length > 0) {
    promises.push(saveToDynamoDB(items))
  }

  if (promises.length > 0) {
    await Promise.all(promises)
  }

  console.log('Total processed lines:', i)
}

async function downloadFile () {
  const file = await s3.getObject(s3Params).promise()
  fs.writeFileSync(ZIP_NAME, file.Body)
}

async function unzipFile () {
  await extract(ZIP_NAME, {
    dir: CSV_TARGET_DIR
  })
}

module.exports = {
  downloadFile,
  unzipFile,
  processFile
}

// aws lambda invoke --region us-east-2 \
// --function-name profiles-test-dev-loadProfiles out --log-type Tail \
// --query 'LogResult' --output text |  base64 -d
