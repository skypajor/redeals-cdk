import {AttributeValue, DynamoDBClient, PutItemCommand, PutItemInput} from '@aws-sdk/client-dynamodb';
import * as fs from 'fs';
import * as csv from 'csv-parser';

// Set the AWS region
const REGION = "us-west-2";
// Set they Dynamo Table Name
const TABLE_NAME = "RedealsCdkStack-ReDealsTable4A35F7E1-R3OWVODXX0JK";

async function csvToJson(filePath: string): Promise<any[]>
{
  const result: any[] = [];
  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => result.push(data))
      .on('end', () => {
        resolve(result);
      })
      .on('error', (error) => {
        reject(error);
      });
  });
}

function putItem(ddbClient: DynamoDBClient, tableName: string, item: Record<string, AttributeValue>) {
  const params: PutItemInput = {
    TableName: tableName,
    Item: item,
  };

  try {
    const data =  ddbClient.send(new PutItemCommand(params));
  } catch (err) {
    console.error(err);
  }
}

async function main() {
  try {
    const data = await csvToJson('redeals.csv');

    const ddbClient = new DynamoDBClient({ region: REGION });
    for (const item of data) {
      putItem(ddbClient, TABLE_NAME, {
        id: { N: item.id },
        listed_date: { S: item.listed_date },
        units: { N: item.units },
        address: { S: item.address },
        city: { S: item.city },
        market_rent: { N: item.market_rent },
        list_price: { N: item.list_price },
        sold_price: { N: item.sold_price },
        sqft: { N: item.sqft },
        repairs_maintenance: { N: item.repairs_maintenance },
        landscape: { N: item.landscape },
        insurance: { N: item.insurance },
        gas_electric: { N: item.gas_electric },
        garbage_water_sewage: { N: item.garbage_water_sewage },
        tax: { N: item.tax },
        vacancy_rate: { N: item.vacancy_rate },
        property_management: { N: item.property_management },
        rent: { N: item.rent },
        other_inc: { N: item.other_inc }
      });
    }
  } catch (error) {
    console.error("An error occurred: ", error);
  }
}

main();


