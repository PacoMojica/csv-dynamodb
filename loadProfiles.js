const { processFile, downloadFile, unzipFile } = require('./lib')

const handle = async event => {
  const startTime = Date.now()

  try {
    console.log('Download started')
    const fileName = '/tmp/asdf.zip'
    await downloadFile('https://aws-techmentoring-paco.s3.amazonaws.com/asdf.zip', fileName)
    console.log('Download done')

    console.log('Unzip started')
    await unzipFile(fileName, '/tmp')
    console.log('Unzip done')

    console.log('Start loading postcodes into DynamoDB')
    await processFile('/tmp/asdf.csv')
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
