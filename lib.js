const AWS = require('aws-sdk')
const fs = require('fs')
const readline = require('readline')
const https = require('https')
const extract = require('extract-zip')

const batchSize = 25
const concurrentRequests = 40

process.env.AWS_NODEJS_CONNECTION_REUSE_ENABLED = 1

const dynamodb = new AWS.DynamoDB.DocumentClient()

function isNumeric(str) {
  return (
    typeof str == "string" &&
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
    age: isNumeric(items[10]) ? parseInt(items[10]) : '',
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
      ['accounts_profiles_dev']: putReqs
    }
  }

  await dynamodb.batchWrite(req).promise()
}

async function processFile (fileName) {
  const readStream = fs.createReadStream(fileName, { encoding: 'utf8' })
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

    const obj = convertToObject(line)
    if (obj) {
      items.push(convertToObject(line))
    }

    if (items.length % batchSize === 0) {
      promises.push(saveToDynamoDB(items))

      if (promises.length % concurrentRequests === 0) {
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

async function downloadFile (url, fileName) {
  const writableStream = fs.createWriteStream(fileName)
  return new Promise((resolve, reject) => {
    const request = https.get(url, function (response) {
      response.pipe(writableStream)
      response.on('end', () => {
        writableStream.close()
        resolve()
      })
    })

    request.on('error', reject)
  })
}

async function unzipFile (fileName, targetDir) {
  await extract(fileName, {
    dir: targetDir
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