from flask import Flask, request, redirect, render_template
from s3.s3 import S3Bucket
from werkzeug import secure_filename
from dynamo.dynamo import Dynamo
application = Flask(__name__)
ar = "arn:aws:lambda:eu-west-1:987117698402:function:LoadData-LoadDataFunction-10CMTFASSGL8Z"
@application.route('/', methods=["GET", "POST"])
def home():
    if request.method == "POST":
        
        csv_files = request.files['file_input']
        status = False
        if '.csv' in csv_files.filename:
            s3 = S3Bucket()
            file_name = secure_filename(csv_files.filename)
            status = s3.upload_file(csv_files, object_name=file_name)
        return render_template('upload_file.html', status_upload=status)
    else:
        return render_template('upload_file.html')




@application.route('/logs', methods=["GET"])
def logs():
    if request.method == 'GET':
        d = Dynamo('data_log_process')
        data = d.scan()
        data_id = [item['id'] for item in data]
        column_info = [{data_id[i]:item['column_info']} for i,item in enumerate(data)]
        print(column_info)
        return render_template('log_erros.html', _data=data_id, column_info = column_info)
    else:
        return 'method not allowed'
        

@application.route('/logs/<id>/', methods=["GET"])
def logs_id(id):
    """
    :param
        id: id do arquivo  (nome)
    :return
        template com log do arquivo carregado
    """
    if request.method == 'GET':
        d = Dynamo('data_log_process')
        data = d.get_info(id)    
        data = data.get('column_info')
        print(data)
        return render_template('detail_item.html',id=id, _data=data)
    else:
        return 'method not allowed'
    
if __name__ == '__main__':
    application.debug = True
    application.run()
 

