const AWS = require('aws-sdk')
AWS.config.update({ region: process.env.REGION })
const s3 = new AWS.S3();
var moment = require("moment-timezone");
const http = require('http');
const converter = require('json-2-csv');

exports.handler = (event,context,callback) => {
    
  
  //Definición fechas de busqueda
  let ts = Date.now();
  var date = new Date(ts);
  var lastDateSaved = new Date();

   var param = {
       Bucket: "domina-data-cdr",
       Key : "lastdate.txt"
    };
    
  
    s3.getObject(param, (err, s3file) => {
        if(err) {
       } else {
           lastDateSaved = s3file.Body.toString();
           console.log("Última fecha archivo guardado : ", lastDateSaved)
       }
    })
    
    
 
  setTimeout(function() {
        
     var dateStart= lastDateSaved;
     var dateEnd= moment(date.getTime()).tz("America/Bogota").format("YYYYMMDDHHmmss");
    
     let dataString = '';
   
   const response = new Promise((resolve, reject) => {
        //Definir objeto para API ANS
        const url = 'http://35.245.132.133/ipdialbox/api_reports.php?token=7b69645f6469737472697d2d3230323130373232313532313431&report=cbps_cdr&date_ini='+dateStart+'&date_end='+dateEnd;
        console.log("2" + url);
  
        const req = http.get(url, function(res) {
          res.on('data', chunk => {
            dataString += chunk;
          });
          res.on('end', () => {
            resolve({
                statusCode: 200,
                body: JSON.stringify(JSON.parse(dataString), null, 4)
            });
          console.log(dataString)
          if(dataString.toString().includes("null")) {
           
             console.log("No responseCdr")

           }else{
               
                converter.json2csv(JSON.parse(dataString), (err, data) => {
                    if (err) {
                        throw err;
                    }
                     // Subir archivo Historico a S3
                     uploadObjectToS3Bucket("domina-data-historic", "CDR-"+dateEnd+".csv", data);
                     // Subir archivo ANS a S3
                     uploadObjectToS3Bucket("domina-data-cdr", "LastDataCdr.csv", data);
                     // Subir archivo con última fecha de guardado a S3
                     uploadObjectToS3Bucket("domina-data-cdr", "lastdate.txt", dateEnd);

                });
             
           }


          });
        });
        
        req.on('error', (e) => {
          reject({
              statusCode: 500,
              body: 'Something went wrong Ans!'
          });
        });
    });
      return response;

}, 500);
  
};



function uploadObjectToS3Bucket(objectNameFinal, objectName, objectData) {
    
    
    return new Promise((resolve, reject) => {
        
         const params = {
            ACL:"public-read",
            ContentType: 'text/csv',
            Bucket: objectNameFinal,
            Key: objectName,
            Body: objectData
          };
          
         s3.upload(params, (err, data) => 
              {
              console.log('putObject callback executing');
               if (err) {
                            console.log("Error: ", err);
                            return reject(err);
                        }
                        if (data) {
                            console.log("Success: ", data.Location);
                            return resolve();   
                        }
              });
    });
}