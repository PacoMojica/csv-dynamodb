const { processFile, downloadFile, unzipFile } = require('./lib')

const handle = async event => {
  const startTime = Date.now()

  try {
    console.log('Download started')
    await downloadFile()
    console.log('Download done')

    console.log('Unzip started')
    await unzipFile()
    console.log('Unzip done')

    console.log('Start loading postcodes into DynamoDB')
    await processFile()
  } catch (error) {
    console.error(error)
  }

  const timeTakenSecs = (Date.now() - startTime) / 1000
  console.log(`Job done in ${timeTakenSecs} seconds`)

  return {
    statusCode: 200,
    body: JSON.stringify(
      {
        message: `profiles loaded into DynamoDB successfully in ${timeTakenSecs} seconds`
      }
    )
  }
}

module.exports = {
  handle
}
