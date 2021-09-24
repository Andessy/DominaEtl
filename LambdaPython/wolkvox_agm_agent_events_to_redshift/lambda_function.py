import boto3  #para usar s3
import psycopg2 #para la conexion con redshift

def lambda_handler(event, context):
    s3 = boto3.client("s3")
    if event:
        # para extraer informacion del evento de disparo
        file_obj = event["Records"][0]
        time = str(file_obj['eventTime'])
        bucketname = str(file_obj['s3']['bucket']['name'])
        filename = str(file_obj['s3']['object']['key'])
        if(filename[-3:] == "csv"): #para asegurar que sea el archivo csv el que se va a copiar a redshift
            conn_string = "dbname=" + "dev" + " port=" + "5439" + " user=" + "awsuser" + " password=" + "DominaBI2021" + " host=" + "cluster-domina-glue.c46hufyrfyvq.us-east-1.redshift.amazonaws.com";
            connection = psycopg2.connect(conn_string)
            iam_role = "'arn:aws:iam::963212646939:role/AWSRedshiftScheduler'"
            table = "agent_events"    
            port = "5439"
            delimiter = "','"
            region = "'us-east-1'"
            copy_query = "COPY "+table+" from 's3://"+ bucketname+'/'+filename +"' iam_role "+ iam_role+" delimiter "+delimiter + "ignoreheader 1" + ";"
            with connection.cursor() as cur:
                cur.execute(copy_query) 
                connection.commit()
                cur.close()