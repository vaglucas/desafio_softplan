from flask import Flask
from flask import Flask, request, redirect, render_template
from s3.s3 import S3Bucket
from werkzeug import secure_filename

app = Flask(__name__)
ar = "arn:aws:lambda:eu-west-1:987117698402:function:LoadData-LoadDataFunction-10CMTFASSGL8Z"
@app.route('/', methods=["GET", "POST"])
def home():
    if request.method == "POST":
        print(request.files)
        csv_files = request.files['file_input']
        print(csv_files)
        s3 = S3Bucket()
        file_name = secure_filename(csv_files.filename)
        status = s3.upload_file(csv_files, object_name=file_name)
        return render_template('upload_file.html', status_upload=status)
    else:
        return render_template('upload_file.html')




@app.route('/logs', methods=["GET"])
def logs():

    return render_template('upload_file.html')

