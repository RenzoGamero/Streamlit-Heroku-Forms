# Domigngo 12 de setiembre
import pandas as pd
import numpy as np
import os
import pickle
import os.path
import io
import shutil
import streamlit as st
from datetime import datetime
import datetime as dt
from mimetypes import MimeTypes
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import time
import pygsheets
import string
import operator
from PIL import Image
from io import BytesIO
import glob
class DriveAPI:
    global SCOPES
    SCOPES = ['https://www.googleapis.com/auth/drive']

    # SCOPES = ['https://mail.google.com/']
    def __init__(self):

        # Variable self.creds will
        # store the user access token.
        # If no valid token found
        # we will create one.
        self.creds = None

        # The file token.pickle stores the
        # user's access and refresh tokens. It is
        # created automatically when the authorization
        # flow completes for the first time.

        # Check if file token.pickle exists
        if os.path.exists('token.pickle'):
            # Read the token from the file and
            # store it in the variable self.creds
            with open('token.pickle', 'rb') as token:
                self.creds = pickle.load(token)

        # If no valid credentials are available,
        # request the user to log in.
        if not self.creds or not self.creds.valid:

            # If token is expired, it will be refreshed,
            # else, we will request a new one.
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                self.creds = flow.run_local_server(port=0)

            # Save the access token in token.pickle
            # file for future usage
            with open('token.pickle', 'wb') as token:
                pickle.dump(self.creds, token)

        # Connect to the API service
        self.service = build('drive', 'v3', credentials=self.creds, cache_discovery=False)

        # request a list of first N files or
        # folders with name and id from the API.
        results = self.service.files().list(
            pageSize=100, fields="files(id, name)").execute()
        items = results.get('files', [])

        # print a list of files

        ##print("Here's a list of files: \n")
        ##print(*items, sep="\n", end="\n\n")

    def FileDownload(self, file_id, file_name):
        request = self.service.files().get_media(fileId=file_id)
        fh = io.BytesIO()

        # Initialise a downloader object to download the file
        downloader = MediaIoBaseDownload(fh, request, chunksize=404800)
        done = False

        # try:
        # Download the data in chunks
        while not done:
            status, done = downloader.next_chunk()

        fh.seek(0)

        # Write the received data to the file
        with open(file_name, 'wb') as f:
            shutil.copyfileobj(fh, f)

        print("File Downloaded")
        # Return True if file Downloaded successfully
        return True
        # except:

        #    # Return False if something went wrong
        #    print("Something went wrong.")
        #    return False

    def upload_new_local_file(self, filepath, folder_id):
        """
        Upload a new file to a Google Drive folder from local.
        Parameters
        ----------
        drive: GoogleDrive object
        filename : str
            the name will be used on the Google Drive
        folder_id : str
            parent folder File ID
        Returns
        -------
        str
            uploaded file's File ID
        """
        name = filepath.split('/')[-1]

        # Find the MimeType of the file
        mimetype = MimeTypes().guess_type(name)[0]

        folder_id = '1n7QL0fvuXiRg6OeVU7OnFq3MdHpQG6Is'
        file_metadata = {
            'name': name,
            'parents': [folder_id]
        }

        # try:
        media = MediaFileUpload(filepath, mimetype=mimetype)

        # print('name             = ', name)
        # print('mimetype         = ', mimetype)
        # print('filepath         = ', filepath)
        # print('file_metadata    = ', file_metadata)

        # print('media = ', media)

        # Create a new file in the Drive storage
        file = self.service.files().create(
            body=file_metadata, media_body=media, fields='id').execute()
        print('         File Created')

    def FileDownload2(self, file_id, file_name):
        # file_id = [File id from first call]

        # request = self.service.files().get_media(fileId=file_id)
        request = self.service.files().export_media(fileId=file_id, mimeType='text/csv')
        request = self.service.files().export_media(fileId=file_id,
                                                    mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

        fh = io.BytesIO()
        # print('request  ', request )
        # print('fh       ', fh )

        # Initialise a downloader object to download the file
        downloader = MediaIoBaseDownload(fh, request)
        done = False

        # try:
        # Download the data in chunks
        while not done:
            status, done = downloader.next_chunk()

        fh.seek(0)

        # Write the received data to the file
        with open(file_name, 'wb') as f:
            shutil.copyfileobj(fh, f)

        print("File Downloaded")
        # Return True if file Downloaded successfully
        return True
        # except:

        #    # Return False if something went wrong
        #    print("Something went wrong.")
        #    return False

    def FileDownload3(self, file_id, file_name):
        # file_id = [File id from first call]
        request = self.service.files().export_media(fileId=file_id,
                                                    mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document')

        fh = io.BytesIO()
        # print('request  ', request )
        # print('fh       ', fh )

        # Initialise a downloader object to download the file
        downloader = MediaIoBaseDownload(fh, request)
        done = False

        # try:
        # Download the data in chunks
        while not done:
            status, done = downloader.next_chunk()

        fh.seek(0)

        # Write the received data to the file
        with open(file_name, 'wb') as f:
            shutil.copyfileobj(fh, f)

        print("File Downloaded")
        # Return True if file Downloaded successfully
        return True
        # except:

        #    # Return False if something went wrong
        #    print("Something went wrong.")
        #    return False

    def update_file(self, file_id, new_title, new_description, new_mime_type,
                    new_filename, new_revision, file):
        """Update an existing file's metadata and content.

        Args:
        service: Drive API service instance.
        file_id: ID of the file to update.
        new_title: New title for the file.
        new_description: New description for the file.
        new_mime_type: New MIME type for the file.
        new_filename: Filename of the new content to upload.
        new_revision: Whether or not to create a new revision for this file.
        Returns:
        Updated file metadata if successful, None otherwise.
        """
        # try:
        # First retrieve the file from the API.
        # file = self.service.files().get(fileId=file_id).execute()

        name = new_filename.split('/')[-1]

        # Find the MimeType of the file
        mimetype = MimeTypes().guess_type(name)[0]
        new_mime_type = mimetype
        # File's new metadata.
        # file['title'] = new_title
        # file['description'] = new_description
        # file['mimeType'] = new_mime_type

        # File's new content.
        media_body = MediaFileUpload(new_filename, mimetype=new_mime_type, resumable=True)

        # Send the request to the API.
        updated_file = self.service.files().update(
            fileId=file_id,
            body=file,
            # newRevision=new_revision,
            media_body=media_body
        ).execute()
        return updated_file
        # except errors.HttpError, error:
        # print('An error occurred: %s', error)
        # return None

    def FileUpload(self, filepath, file_id):

        # Extract the file name out of the file path
        name = filepath.split('/')[-1]

        # Find the MimeType of the file
        mimetype = MimeTypes().guess_type(name)[0]

        # create file metadata
        file_metadata = {'name': str("") + name}
        folder_id = '1n7QL0fvuXiRg6OeVU7OnFq3MdHpQG6Is'
        file_metadata = {
            'name': name,
            'parents': [folder_id]
        }

        # try:
        media = MediaFileUpload(filepath, mimetype=mimetype)

        # print('name             = ', name)
        # print('mimetype         = ', mimetype)
        # print('filepath         = ', filepath)
        # print('file_metadata    = ', file_metadata)

        # print('media = ', media)

        # Create a new file in the Drive storage
        # file = self.service.files().update(
        #    body=file_metadata, media_body=media, fields='id').execute()
        updated_file = self.service.files().update(
            fileId=file_id,
            body=filepath,
            # addParents='1n7QL0fvuXiRg6OeVU7OnFq3MdHpQG6Is',
            # body=file_metadata,
            # media_body = media, fields = 'id'
            # newRevision=new_revision,
            # media_body=media_body
        ).execute()

        # body = file_metadata, media_body = media, fields = 'id').execute()

        print("         File Uploaded.")

        # except:
        #    print("Can't Upload File.")
        # Raise UploadError if file is not uploaded.
        # raise UploadError("Can't Upload File.")

    def FileDelete(self, filepath, file_id):

        # Extract the file name out of the file path
        name = filepath.split('/')[-1]

        # Find the MimeType of the file
        mimetype = MimeTypes().guess_type(name)[0]

        # create file metadata
        file_metadata = {'name': str("") + name}
        folder_id = '1n7QL0fvuXiRg6OeVU7OnFq3MdHpQG6Is'
        file_metadata = {
            'name': name,
            'parents': [folder_id]
        }

        # try:
        media = MediaFileUpload(filepath, mimetype=mimetype)

        # print('name             = ', name)
        # print('mimetype         = ', mimetype)
        # print('filepath         = ', filepath)
        # print('file_metadata    = ', file_metadata)

        # print('media = ', media)

        # Create a new file in the Drive storage
        # file = self.service.files().update(
        #    body=file_metadata, media_body=media, fields='id').execute()
        updated_file = self.service.files().delete(
            fileId=file_id,
            # body=filepath,
            # addParents='1n7QL0fvuXiRg6OeVU7OnFq3MdHpQG6Is',
            # body=file_metadata,
            # media_body = media, fields = 'id'
            # newRevision=new_revision,
            # media_body=media_body
        ).execute()

        # body = file_metadata, media_body = media, fields = 'id').execute()

        print("         File deleted.")

        # except:
        #    print("Can't Upload File.")
        # Raise UploadError if file is not uploaded.
        # raise UploadError("Can't Upload File.")

    def print_files_in_folder2(self, folder_id):
        kwargs = {
            "q": "'{}' in parents".format(folder_id),
            # Specify what you want in the response as a best practice. This string
            # will only get the files' ids, names, and the ids of any folders that they are in
            "fields": "nextPageToken,incompleteSearch,files(id,parents,name)",
            # Add any other arguments to pass to list()
        }
        request = self.service.files().list(**kwargs)
        while request is not None:
            response = request.execute()
            # Do stuff with response['files']
            request = self.service.files().list_next(request, response)
            # print('request= ', request)
            # print('response1        ', response)
            # print('response2        ', response['files'])
            # print('response3 len    ', len(response['files']))

            # print('response3        ', response['files'][0])
            for i in range(len(response['files'])):
                # print(i,'responsei        ', response['files'][i]['id'])

                f_path = response['files'][i]['name']
                fileid = response['files'][i]['id']
                obj = DriveAPI()
                obj.FileDelete(f_path, fileid)

    def print_files_in_folder3(self, folder_id):
        kwargs = {
            "q": "'{}' in parents".format(folder_id),
            # Specify what you want in the response as a best practice. This string
            # will only get the files' ids, names, and the ids of any folders that they are in
            "fields": "nextPageToken,incompleteSearch,files(id,parents,name)",
            # Add any other arguments to pass to list()
        }
        request = self.service.files().list(**kwargs)
        while request is not None:
            response = request.execute()
            # Do stuff with response['files']
            request = self.service.files().list_next(request, response)
            # print('request= ', request)
            # print('response1        ', response)
            # print('response2        ', response['files'])
            # print('response3 len    ', len(response['files']))

            # print('response3        ', response['files'][0])
            for i in range(len(response['files'])):
                # print(i,'responsei        ', response['files'][i]['id'])
                if (response['files'][i]['name'] == 'Consistencia - CAR.xlsx'):
                    f_path = response['files'][i]['name']
                    fileid = response['files'][i]['id']
                    obj = DriveAPI()
                    obj.FileDelete(f_path, fileid)

    def FolderSearch2(self, folder_id):
        kwargs = {
            "q": "'{}' in parents".format(folder_id),
            # Specify what you want in the response as a best practice. This string
            # will only get the files' ids, names, and the ids of any folders that they are in
            "fields": "nextPageToken,incompleteSearch,files(id,parents,name,mimeType)",
            # 'mimeType': 'application/vnd.google-apps.folder'
            # Add any other arguments to pass to list()
        }
        request = self.service.files().list(**kwargs)
        while request is not None:
            response = request.execute()
            # Do stuff with response['files']
            request = self.service.files().list_next(request, response)

            # print('request= ', request)
            # response = response.encode('utf-8', 'ignore').decode('utf-8')
            # print('response1        ', response)

            # print('response1        ', type(response['files']))
            # print('response1        ', response['files'])
            df = pd.DataFrame(response['files'])

            print(df.head())

            # print('response2        ', response['files'])
            # print('response3 len    ', len(response['files']))

            # print('response3        ', response['files'][0])
            for i in range(len(response['files'])):
                f = response['files'][i]
                # f = f.decode('utf-8')
                print('--> ', str(f))
        return df

    def FolderCreator(self, name2, folder_id):

        # request = self.service.files().export_media(fileId=file_id,
        #     mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document')

        # Extract the file name out of the file path
        # name = filepath.split('/')[-1]

        # Find the MimeType of the file
        # mimetype = MimeTypes().guess_type(name)[0]
        # print('mimetype= ', mimetype )
        # create file metadata
        file_metadata = {'name': name2, 'parents': [folder_id], 'mimeType': 'application/vnd.google-apps.folder'}

        # application/vnd.google-apps.folder
        """
        file_metadata = {
            'name': 'Invoices',
            'mimeType': 'application/vnd.google-apps.folder'
        }
        file = drive_service.files().create(body=file_metadata,
                                            fields='id').execute()
        print
        'Folder ID: %s' % file.get('id')
        """
        try:
            # media = MediaFileUpload(filepath, mimetype='application/vnd.google-apps.folder')

            # Create a new file in the Drive storage
            file = self.service.files().create(
                body=file_metadata, fields='webViewLink, id').execute()

            # print('file.path= ', file.path)

            # print('File ID: %s' % file.get('id'))
            # print(file.get('webViewLink'))
            # new = DRIVE.files().create(body=data, fields='webViewLink, id').execute()
            # return new.get('webViewLink')
            print("carpeta creada")

            return file.get('webViewLink'), file.get('id')



        except:
            print('Error al crear carpteta ')
            # Raise UploadError if file is not uploaded.
            # raise UploadError("Can't Upload File.")

    def FileUpload_(self, name2, filepath, folder_id):
        print('Inicio de FileUpload_ ------------------------------------------')
        # Extract the file name out of the file path
        print('name2= ', name2)
        print('filepath= ', filepath)
        print('folder_id= ', folder_id)
        #        print('name= ', name2[0])
        name = filepath.split('/')[-1]

        # Find the MimeType of the file
        mimetype = MimeTypes().guess_type(name)[0]
        # print('mimetype= ', mimetype )
        # create file metadata
        print('name= ', name2[0] )
        print('parents= ', [folder_id] )

        file_metadata = {'name': name2[0], 'parents': [folder_id]}


        #try:

        media = MediaFileUpload(filepath, mimetype=mimetype)
        # Create a new file in the Drive storage
        file = self.service.files().create(
            body=file_metadata, media_body=media, fields='webViewLink, id').execute()
        # print('file.path= ', file.path)

        # print('File ID: %s' % file.get('id'))
        # print(file.get('webViewLink'))
        # new = DRIVE.files().create(body=data, fields='webViewLink, id').execute()
        # return new.get('webViewLink')
        print("File Uploaded.")
        print('Fin de FileUpload_ ------------------------------------------')
        return file.get('webViewLink')

        #except:

        #print('Error FileUpload_ ')
        #print('Fin de FileUpload_ ------------------------------------------')
        # Raise UploadError if file is not uploaded.
        # raise UploadError("Can't Upload File.")

def date_expander(dataframe: pd.DataFrame, start_dt_colname: str, end_dt_colname: str,
                          time_unit: str, new_colname: str, end_inclusive: bool):
            td = pd.Timedelta(1, time_unit)

            # add a timediff column:
            dataframe['_dt_diff'] = dataframe[end_dt_colname] - dataframe[start_dt_colname]

            # get the maximum timediff:
            max_diff = int((dataframe['_dt_diff'] / td).max())

            # for each possible timediff, get the intermediate time-differences:
            df_diffs = pd.concat(
                [pd.DataFrame({'_to_add': np.arange(0, dt_diff + end_inclusive) * td}).assign(_dt_diff=dt_diff * td)
                 for dt_diff in range(max_diff + 1)])

            # join to the original dataframe
            data_expanded = dataframe.merge(df_diffs, on='_dt_diff')

            # the new dt column is just start plus the intermediate diffs:
            data_expanded[new_colname] = data_expanded[start_dt_colname] + data_expanded['_to_add']

            # remove start-end cols, as well as temp cols used for calculations:
            to_drop = [start_dt_colname, end_dt_colname, '_to_add', '_dt_diff']
            if new_colname in to_drop:
                to_drop.remove(new_colname)
            data_expanded = data_expanded.drop(columns=to_drop)

            # don't modify dataframe in place:
            del dataframe['_dt_diff']

            return data_expanded

def expanderrrbackup(x, q, op, tipo, qe):
    print('=============================================================')
    print('X= ', x)
    print('q= ', q)
    print('qe= ', qe)
    if tipo == 'selectbox':
        if qe == '':
            option = st.selectbox(q, op)
            st.write('Seleccionaste:', option)
            df['resp'][x] = option
        else:
            if df[(df['q_'] == qe)]['resp'].values[0] == 'Si':
                option = st.selectbox(q, op)
                st.write('Seleccionaste:', option)
                df['resp'][x] = option
    if tipo == 'multiselect':
        if qe == '':
            options = st.multiselect(q, op)
            st.write('Seleccionaste:', options)
            df['resp'][x] = str(options)
        else:
            if df[(df['q_'] == qe)]['resp'].values[0] == 'Si':
                options = st.multiselect(q, op)
                st.write('Seleccionaste:', options)
                df['resp'][x] = str(options)
    if tipo == 'text_input':
        if qe == '':
            titles = st.text_input(q, key='text_input1' + str(page))
            st.write('Seleccionaste:', titles)
            df['resp'][x] = titles
        else:
            if df[(df['q_'] == qe)]['resp'].values[0] == 'Si':
                titles = st.text_input(q, key='text_input2' + str(page))
                st.write('Seleccionaste:', titles)
                df['resp'][x] = titles
    if tipo == 'radio':
        if qe == '':
            genre = st.radio(q, (op))
            st.write('Seleccionaste:', genre)
            df['resp'][x] = genre
        else:
            print('--->', df[(df['q'] == qe)]['resp'].values[0])
            if df[(df['q_'] == qe)]['resp'].values[0] == 'Si':
                genre = st.radio(q, (op))
                st.write('Seleccionaste:', genre)
                df['resp'][x] = genre
    if tipo == 'date_input':
        if qe == '':
            Fecha = st.date_input(q,
                                  min_value=dt.datetime.today() + dt.timedelta(days=-30),
                                  max_value=dt.datetime.today() + dt.timedelta(days=14))
            st.write('Fecha:', Fecha)
            # tr = datetime.strptime(dt.datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d').date()
            # tr = datetime.strptime(Fecha.strftime('%Y-%m-%d'), '%Y-%m-%d')

            df['resp'][x] = str(Fecha.strftime('%Y-%m-%d'))


        else:
            # print('--->', df[(df['q'] == qe)]['resp'].values[0])
            if df[(df['q_'] == qe)]['resp'].values[0] == 'Si':
                Fecha = st.date_input(q,
                                      min_value=dt.datetime.today() + dt.timedelta(days=-30),
                                      max_value=dt.datetime.today() + dt.timedelta(days=14))
                st.write('Fecha:', Fecha)
                df['resp'][x] = str(Fecha.strftime('%Y-%m-%d'))
    if tipo == 'number_input':
        print('tipo  number_input')
        print(df)
        if qe == '':
            print('qe vacio ')
            number = st.number_input(q, step=1)
            st.write('Seleccionaste: ', number)
            df['resp'][x] = number
        else:

            if df[(df['q_'] == qe)]['resp'].values[0] == 'Si':
                number = st.number_input(q, step=1)
                st.write('Seleccionaste: ', number)
                df['resp'][x] = number
    if tipo == 'number_input%':
        if qe == '':
            number = st.number_input(q, max_value=100)
            st.write('Seleccionaste: ', number, ' %')
            df['resp'][x] = number
        else:
            # print('--->', df[ (df['q'] == qe)]['resp'].values[0])
            if df[(df['q_'] == qe)]['resp'].values[0] == 'Si':
                number = st.number_input(q, max_value=100)
                st.write('Seleccionaste: ', number, ' %')
                df['resp'][x] = number


def expanderrr(x, q, op, tipo, Dependencia, nivel, vista, DependenciaSiNo, Validar, Dependencia_Respuesta):
    print('=============================================================')
    #print('1X= ', x)
    #print('1q= ', q)
    #print('1nivel= ', nivel)
    #print('1op= ', op)
    #print('Dependencia= ', Dependencia)

    # print('Dependencia= |', Dependencia,'|', len(Dependencia))
    # print('DependenciaSiNo= |', DependenciaSiNo,'|', len(Dependencia))

    global df
    if tipo == 'selectbox':

        import operator
        ops = {"+": operator.add,
               "-": operator.sub,
               "<": operator.lt,
               "<=": operator.le,
               "==": operator.eq,
               "!=": operator.ne,
               ">": operator.gt,
               ">=": operator.ge
               }  # etc.

        if DependenciaSiNo == '' and Dependencia_Respuesta == '':
            print('----if')
            option = st.selectbox(q, op)
            st.write('Seleccionaste:', option)
            df['resp'][x] = option
        else:
            print('----else')
            # print('df[(df[q_] == Dependencia)][resp].values[0]= ', df[(df['q_'] == Dependencia)]['resp'].values[0])
            # print('str(DependenciaSiNo)= ', str(DependenciaSiNo))#
            # print('Dependencia_Respuesta =  ', Dependencia_Respuesta)
            try:
                if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(
                        DependenciaSiNo) and Dependencia_Respuesta == '':
                    print('entro a if ')
                    option = st.selectbox(q, op)
                    st.write('Seleccionaste:', option)
                    df['resp'][x] = option
            except:
                print('Error en primer If ')

            if Dependencia_Respuesta != '':
                print('entro a else ')
                StringOperator = ''.join([i for i in Dependencia_Respuesta if not i.isdigit()])
                PreguntaObj = ''.join([i for i in Dependencia_Respuesta if i.isdigit()])
                print('StringOperator           = ', StringOperator)
                print('PreguntaObj              = ', PreguntaObj)
                import re
                a = ['<', '>', '=', '==', '<=', '>=']
                aal = ['and', 'or']
                a1 = re.split(' ', StringOperator)
                list_a = (aal)
                list_b = set(a1)

                # list_a=[[el] for el in a]
                # list_b=[[el] for el in a1]
                print('list_a= ', list_a)
                print('list_b= ', list_b)

                set2 = set(list_b)

                result = [x for x in list_a if x[0] in list_b]
                result2 = filter(lambda list_a: list_a[0] in list_b, list_a)
                result3 = [x for x in list_a if x in set2]
                print('result= ', result)
                print('result2= ', result2)
                print('result2= ', result3)

                print('Dependencia_Respuesta1    = ', Dependencia_Respuesta)

                DR = re.split('and|or', Dependencia_Respuesta)
                print('Dependencia_Respuesta2    = ', DR)
                DR_3 = [s.replace(" ", "") for s in DR]
                print('Dependencia_Respuesta3  DR_3  = ', DR_3)
                DR1 = re.split('<|>|=|==|>=|<=', Dependencia_Respuesta)
                print('Dependencia_Respuesta4   = ', DR1)

                indeces = [i for i, x in enumerate(Dependencia_Respuesta) if x in a]
                print('indeces= ', indeces)
                from itertools import count
                zipped = [(i, j) for i, j in zip(count(), a) if j == Dependencia_Respuesta]
                print('zipped= ', zipped)

                StringOperator = ''.join([i for i in Dependencia_Respuesta if not i.isalpha()])
                PreguntaObj = ''.join([i for i in Dependencia_Respuesta if i.isalpha()])
                print('StringOperator   isalpha         = ', StringOperator)
                print('PreguntaObj      isalpha         = ', PreguntaObj)

                StringOperator1 = ''.join([i for i in StringOperator if not i.isdigit()])
                PreguntaObj1 = ''.join([i for i in StringOperator if i.isdigit()])
                print('StringOperator1   isdigit         = ', StringOperator1)
                print('PreguntaObj1      isdigit         = ', PreguntaObj1)
                print('StringOperator1   isdigit  list        = ', list(StringOperator1))
                print('PreguntaObj1      isdigit  list        = ', list(PreguntaObj1))
                DR = [s.replace(" ", ",") for s in StringOperator1]
                print('StringOperator1   isdigit   2      = ', DR)
                DR3 = re.split(' ', StringOperator1)
                print('PreguntaObj1      DR3      = ', DR3)
                without_empty_strings = [string for string in DR3 if string != ""]
                print('without_empty_strings      DR3      = ', without_empty_strings)

                a = ['<', '>', '=', '==', '<=', '>=']

                for i in range(len(a)):
                    DR_3 = [s.replace(a[i], ' ') for s in DR_3]
                print('DR_3= ', DR_3)

                DR_3 = re.split(' ', ' '.join(DR_3))
                DR_3 = [string for string in DR_3 if string != ""]

                print('*' * 50)
                print('DR_3= ', DR_3)
                print('result3= ', result3)
                print('without_empty_strings = ', without_empty_strings)
                print('*' * 50)
                print(list(df.columns))
                df0 = df[['Vars', 'resp', 'tipo']]
                print(df0)

                print('*' * 100)
                lcolnumbertype = df0[df0.tipo == 'number_input']['Vars'].tolist()
                print('lcolnumbertype= ', lcolnumbertype)

                print('*' * 100)

                df1 = df[['Vars', 'resp']]
                df1 = df0[['Vars', 'resp']]

                df1 = df1.set_index('Vars')
                print('df1= ', df1)

                df2 = df1.T
                print('df2=', df2)
                for i in range(len(lcolnumbertype)):
                    print('i= ', i)
                    try:
                        # df2[lcolnumbertype[i]] = df2[lcolnumbertype[i]].fillna(0)
                        df2[lcolnumbertype[i]].astype('int')
                        # df2[lcolnumbertype[i]].astype(str).astype(float).astype(int)
                        # df2[lcolnumbertype[i]].astype('Int32')
                        # df2[lcolnumbertype[i]].astype(float).astype('Int64')
                        df2[lcolnumbertype[i]] = pd.to_numeric(df2[lcolnumbertype[i]], errors='coerce').astype('Int64')

                        # df2[lcolnumbertype[i]].astype(np.float).astype("Int32")
                    except:
                        print('Error de conversion ')
                        print('lcolnumbertype[i]= ', lcolnumbertype[i])
                        print('df.columns= ', df.columns)
                        print(df[['tipo', 'Vars']])
                        print('df2[lcolnumbertype[i]].values= ', df2[lcolnumbertype[i]].values[0])
                        print('_>', df[df.Vars == lcolnumbertype[i]]['tipo'].values[0])

                        print('i df2[lcolnumbertype[i]]=', df2[lcolnumbertype[i]])
                        print('f df2[lcolnumbertype[i]]=', str(df2[lcolnumbertype[i]].values[0]))
                        if str(df[df.Vars == lcolnumbertype[i]]['tipo'].values[0]) == 'number_input':
                            print('dentro de If ')
                            df2[lcolnumbertype[i]] = df2[lcolnumbertype[i]].fillna(0)
                        else:
                            print('Fuera de If ')

                        # df2[lcolnumbertype[i]].astype(np.float).astype("Int32")
                        try:
                            df2[lcolnumbertype[i]].astype(int)
                        except:
                            print('Error de conversion 2')
                            # df2[lcolnumbertype[i]]=0
                print(df2)
                print('--------------- desde text_input1----')
                print('Dependencia_Respuesta= ', Dependencia_Respuesta)
                print('---------------')

                # print('r_car_cama_cun_distanc---->= ', df2['r_car_cama_cun_distanc'] )

                # print('r_car_18_mas_total---->= ', df2['r_car_18_mas_total'] )
                print(df2.dtypes)
                print(df2.eval(Dependencia_Respuesta).values[0])
                print('---------------')

                print('*' * 50)

                # a=1
                # b=1
                # c=2
                # print('a= ',a, ' b= ', b, ' c=', c)
                # if a > 2 and b>2 or c==2:
                #    print('Dentro de If ')
                # else:
                #    print('Dentro de else ')

                # if DependenciaSiNo == '' and Dependencia_Respuesta != '' \
                #        and ops[StringOperator](df[(df['q_'] == int(Dependencia))]['resp'].values[0] ,int(PreguntaObj)):

                if df2.eval(Dependencia_Respuesta).values[0]:
                    # number = st.number_input(q, step=1, min_value=0)
                    # st.write('Seleccionaste: ', number)
                    # df['resp'][x] = number
                    option = st.selectbox(q, op)
                    st.write('Seleccionaste:', option)
                    df['resp'][x] = option
    if tipo == 'multiselect':

        import operator
        ops = {"+": operator.add,
               "-": operator.sub,
               "<": operator.lt,
               "<=": operator.le,
               "==": operator.eq,
               "!=": operator.ne,
               ">": operator.gt,
               ">=": operator.ge
               }  # etc.

        if DependenciaSiNo == '' and Dependencia_Respuesta == '':
            print('----if')
            options = st.multiselect(q, op)
            st.write('Seleccionaste:', options)
            df['resp'][x] = str(options)
        else:
            print('----else')
            # print('df[(df[q_] == Dependencia)][resp].values[0]= ', df[(df['q_'] == Dependencia)]['resp'].values[0])
            # print('str(DependenciaSiNo)= ', str(DependenciaSiNo))#
            # print('Dependencia_Respuesta =  ', Dependencia_Respuesta)
            try:
                if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(
                        DependenciaSiNo) and Dependencia_Respuesta == '':
                    print('entro a if ')
                    options = st.multiselect(q, op)
                    st.write('Seleccionaste:', options)
                    df['resp'][x] = str(options)
            except:
                print('Error en primer If ')

            if Dependencia_Respuesta != '':
                print('entro a else ')
                StringOperator = ''.join([i for i in Dependencia_Respuesta if not i.isdigit()])
                PreguntaObj = ''.join([i for i in Dependencia_Respuesta if i.isdigit()])
                print('StringOperator           = ', StringOperator)
                print('PreguntaObj              = ', PreguntaObj)
                import re
                a = ['<', '>', '=', '==', '<=', '>=']
                aal = ['and', 'or']
                a1 = re.split(' ', StringOperator)
                list_a = (aal)
                list_b = set(a1)

                # list_a=[[el] for el in a]
                # list_b=[[el] for el in a1]
                print('list_a= ', list_a)
                print('list_b= ', list_b)

                set2 = set(list_b)

                result = [x for x in list_a if x[0] in list_b]
                result2 = filter(lambda list_a: list_a[0] in list_b, list_a)
                result3 = [x for x in list_a if x in set2]
                print('result= ', result)
                print('result2= ', result2)
                print('result2= ', result3)

                print('Dependencia_Respuesta1    = ', Dependencia_Respuesta)

                DR = re.split('and|or', Dependencia_Respuesta)
                print('Dependencia_Respuesta2    = ', DR)
                DR_3 = [s.replace(" ", "") for s in DR]
                print('Dependencia_Respuesta3  DR_3  = ', DR_3)
                DR1 = re.split('<|>|=|==|>=|<=', Dependencia_Respuesta)
                print('Dependencia_Respuesta4   = ', DR1)

                indeces = [i for i, x in enumerate(Dependencia_Respuesta) if x in a]
                print('indeces= ', indeces)
                from itertools import count
                zipped = [(i, j) for i, j in zip(count(), a) if j == Dependencia_Respuesta]
                print('zipped= ', zipped)

                StringOperator = ''.join([i for i in Dependencia_Respuesta if not i.isalpha()])
                PreguntaObj = ''.join([i for i in Dependencia_Respuesta if i.isalpha()])
                print('StringOperator   isalpha         = ', StringOperator)
                print('PreguntaObj      isalpha         = ', PreguntaObj)

                StringOperator1 = ''.join([i for i in StringOperator if not i.isdigit()])
                PreguntaObj1 = ''.join([i for i in StringOperator if i.isdigit()])
                print('StringOperator1   isdigit         = ', StringOperator1)
                print('PreguntaObj1      isdigit         = ', PreguntaObj1)
                print('StringOperator1   isdigit  list        = ', list(StringOperator1))
                print('PreguntaObj1      isdigit  list        = ', list(PreguntaObj1))
                DR = [s.replace(" ", ",") for s in StringOperator1]
                print('StringOperator1   isdigit   2      = ', DR)
                DR3 = re.split(' ', StringOperator1)
                print('PreguntaObj1      DR3      = ', DR3)
                without_empty_strings = [string for string in DR3 if string != ""]
                print('without_empty_strings      DR3      = ', without_empty_strings)

                a = ['<', '>', '=', '==', '<=', '>=']

                for i in range(len(a)):
                    DR_3 = [s.replace(a[i], ' ') for s in DR_3]
                print('DR_3= ', DR_3)

                DR_3 = re.split(' ', ' '.join(DR_3))
                DR_3 = [string for string in DR_3 if string != ""]

                print('*' * 50)
                print('DR_3= ', DR_3)
                print('result3= ', result3)
                print('without_empty_strings = ', without_empty_strings)
                print('*' * 50)
                print(list(df.columns))
                df0 = df[['Vars', 'resp', 'tipo']]
                print(df0)

                print('*' * 100)
                lcolnumbertype = df0[df0.tipo == 'number_input']['Vars'].tolist()
                print('lcolnumbertype= ', lcolnumbertype)

                print('*' * 100)

                df1 = df[['Vars', 'resp']]
                df1 = df0[['Vars', 'resp']]

                df1 = df1.set_index('Vars')
                print('df1= ', df1)

                df2 = df1.T
                print('df2=', df2)
                for i in range(len(lcolnumbertype)):
                    print('i= ', i)
                    try:
                        # df2[lcolnumbertype[i]] = df2[lcolnumbertype[i]].fillna(0)
                        df2[lcolnumbertype[i]].astype('int')
                        # df2[lcolnumbertype[i]].astype(str).astype(float).astype(int)
                        # df2[lcolnumbertype[i]].astype('Int32')
                        # df2[lcolnumbertype[i]].astype(float).astype('Int64')
                        df2[lcolnumbertype[i]] = pd.to_numeric(df2[lcolnumbertype[i]], errors='coerce').astype('Int64')

                        # df2[lcolnumbertype[i]].astype(np.float).astype("Int32")
                    except:
                        print('Error de conversion ')
                        print('lcolnumbertype[i]= ', lcolnumbertype[i])
                        print('df.columns= ', df.columns)
                        print(df[['tipo', 'Vars']])
                        print('df2[lcolnumbertype[i]].values= ', df2[lcolnumbertype[i]].values[0])
                        print('_>', df[df.Vars == lcolnumbertype[i]]['tipo'].values[0])

                        print('i df2[lcolnumbertype[i]]=', df2[lcolnumbertype[i]])
                        print('f df2[lcolnumbertype[i]]=', str(df2[lcolnumbertype[i]].values[0]))
                        if str(df[df.Vars == lcolnumbertype[i]]['tipo'].values[0]) == 'number_input':
                            print('dentro de If ')
                            df2[lcolnumbertype[i]] = df2[lcolnumbertype[i]].fillna(0)
                        else:
                            print('Fuera de If ')

                        # df2[lcolnumbertype[i]].astype(np.float).astype("Int32")
                        try:
                            df2[lcolnumbertype[i]].astype(int)
                        except:
                            print('Error de conversion 2')
                            # df2[lcolnumbertype[i]]=0
                print(df2)
                print('--------------- desde text_input1----')
                print('Dependencia_Respuesta= ', Dependencia_Respuesta)
                print('---------------')

                # print('r_car_cama_cun_distanc---->= ', df2['r_car_cama_cun_distanc'] )

                # print('r_car_18_mas_total---->= ', df2['r_car_18_mas_total'] )
                print(df2.dtypes)
                print(df2.eval(Dependencia_Respuesta).values[0])
                print('---------------')

                print('*' * 50)

                # a=1
                # b=1
                # c=2
                # print('a= ',a, ' b= ', b, ' c=', c)
                # if a > 2 and b>2 or c==2:
                #    print('Dentro de If ')
                # else:
                #    print('Dentro de else ')

                # if DependenciaSiNo == '' and Dependencia_Respuesta != '' \
                #        and ops[StringOperator](df[(df['q_'] == int(Dependencia))]['resp'].values[0] ,int(PreguntaObj)):

                if df2.eval(Dependencia_Respuesta).values[0]:
                    # number = st.number_input(q, step=1, min_value=0)
                    # st.write('Seleccionaste: ', number)
                    # df['resp'][x] = number
                    options = st.multiselect(q, op)
                    st.write('Seleccionaste:', options)
                    df['resp'][x] = str(options)
    if tipo == 'text_input':
        import operator
        ops = {"+": operator.add,
               "-": operator.sub,
               "<": operator.lt,
               "<=": operator.le,
               "==": operator.eq,
               "!=": operator.ne,
               ">": operator.gt,
               ">=": operator.ge
               }  # etc.

        #if Dependencia == '':
        if DependenciaSiNo == '' and Dependencia_Respuesta == '':
            print('----if')
            titles = st.text_input(q, key='text_input1' + str(page))
            st.write('Seleccionaste:', titles)
            df['resp'][x] = titles
        else:
            print('----else')
            # print('df[(df[q_] == Dependencia)][resp].values[0]= ', df[(df['q_'] == Dependencia)]['resp'].values[0])
            # print('str(DependenciaSiNo)= ', str(DependenciaSiNo))#
            # print('Dependencia_Respuesta =  ', Dependencia_Respuesta)
            try:
                if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(
                        DependenciaSiNo) and Dependencia_Respuesta == '':
                    print('entro a if ')
                    number = st.number_input(q, step=1, min_value=0)
                    st.write('Seleccionaste: ', number)
                    df['resp'][x] = number
            except:
                print('Error en primer If ')

            if Dependencia_Respuesta != '':
                print('entro a else ')
                StringOperator = ''.join([i for i in Dependencia_Respuesta if not i.isdigit()])
                PreguntaObj = ''.join([i for i in Dependencia_Respuesta if i.isdigit()])
                print('StringOperator           = ', StringOperator)
                print('PreguntaObj              = ', PreguntaObj)
                import re
                a = ['<', '>', '=', '==', '<=', '>=']
                aal = ['and', 'or']
                a1 = re.split(' ', StringOperator)
                list_a = (aal)
                list_b = set(a1)

                # list_a=[[el] for el in a]
                # list_b=[[el] for el in a1]
                print('list_a= ', list_a)
                print('list_b= ', list_b)

                set2 = set(list_b)

                result = [x for x in list_a if x[0] in list_b]
                result2 = filter(lambda list_a: list_a[0] in list_b, list_a)
                result3 = [x for x in list_a if x in set2]
                print('result= ', result)
                print('result2= ', result2)
                print('result2= ', result3)

                print('Dependencia_Respuesta1    = ', Dependencia_Respuesta)

                DR = re.split('and|or', Dependencia_Respuesta)
                print('Dependencia_Respuesta2    = ', DR)
                DR_3 = [s.replace(" ", "") for s in DR]
                print('Dependencia_Respuesta3  DR_3  = ', DR_3)
                DR1 = re.split('<|>|=|==|>=|<=', Dependencia_Respuesta)
                print('Dependencia_Respuesta4   = ', DR1)

                indeces = [i for i, x in enumerate(Dependencia_Respuesta) if x in a]
                print('indeces= ', indeces)
                from itertools import count
                zipped = [(i, j) for i, j in zip(count(), a) if j == Dependencia_Respuesta]
                print('zipped= ', zipped)

                StringOperator = ''.join([i for i in Dependencia_Respuesta if not i.isalpha()])
                PreguntaObj = ''.join([i for i in Dependencia_Respuesta if i.isalpha()])
                print('StringOperator   isalpha         = ', StringOperator)
                print('PreguntaObj      isalpha         = ', PreguntaObj)

                StringOperator1 = ''.join([i for i in StringOperator if not i.isdigit()])
                PreguntaObj1 = ''.join([i for i in StringOperator if i.isdigit()])
                print('StringOperator1   isdigit         = ', StringOperator1)
                print('PreguntaObj1      isdigit         = ', PreguntaObj1)
                print('StringOperator1   isdigit  list        = ', list(StringOperator1))
                print('PreguntaObj1      isdigit  list        = ', list(PreguntaObj1))
                DR = [s.replace(" ", ",") for s in StringOperator1]
                print('StringOperator1   isdigit   2      = ', DR)
                DR3 = re.split(' ', StringOperator1)
                print('PreguntaObj1      DR3      = ', DR3)
                without_empty_strings = [string for string in DR3 if string != ""]
                print('without_empty_strings      DR3      = ', without_empty_strings)

                a = ['<', '>', '=', '==', '<=', '>=']

                for i in range(len(a)):
                    DR_3 = [s.replace(a[i], ' ') for s in DR_3]
                print('DR_3= ', DR_3)

                DR_3 = re.split(' ', ' '.join(DR_3))
                DR_3 = [string for string in DR_3 if string != ""]

                print('*' * 50)
                print('DR_3= ', DR_3)
                print('result3= ', result3)
                print('without_empty_strings = ', without_empty_strings)
                print('*' * 50)
                print(list(df.columns))
                df0 = df[['Vars', 'resp', 'tipo']]
                print(df0)

                print('*' * 100)
                lcolnumbertype = df0[df0.tipo == 'number_input']['Vars'].tolist()
                print('lcolnumbertype= ', lcolnumbertype)

                print('*' * 100)

                df1 = df[['Vars', 'resp']]
                df1 = df0[['Vars', 'resp']]

                df1 = df1.set_index('Vars')
                print('df1= ', df1)

                df2 = df1.T
                print('df2=', df2)
                for i in range(len(lcolnumbertype)):
                    print('i= ', i)
                    try:
                        # df2[lcolnumbertype[i]] = df2[lcolnumbertype[i]].fillna(0)
                        df2[lcolnumbertype[i]].astype('int')
                        # df2[lcolnumbertype[i]].astype(str).astype(float).astype(int)
                        # df2[lcolnumbertype[i]].astype('Int32')
                        # df2[lcolnumbertype[i]].astype(float).astype('Int64')
                        df2[lcolnumbertype[i]] = pd.to_numeric(df2[lcolnumbertype[i]], errors='coerce').astype('Int64')

                        # df2[lcolnumbertype[i]].astype(np.float).astype("Int32")
                    except:
                        print('Error de conversion ')
                        print('lcolnumbertype[i]= ', lcolnumbertype[i])
                        print('df.columns= ', df.columns)
                        print(df[['tipo', 'Vars']])
                        print('df2[lcolnumbertype[i]].values= ', df2[lcolnumbertype[i]].values[0])
                        print('_>', df[df.Vars == lcolnumbertype[i]]['tipo'].values[0])

                        print('i df2[lcolnumbertype[i]]=', df2[lcolnumbertype[i]])
                        print('f df2[lcolnumbertype[i]]=', str(df2[lcolnumbertype[i]].values[0]))
                        if str(df[df.Vars == lcolnumbertype[i]]['tipo'].values[0]) == 'number_input':
                            print('dentro de If ')
                            df2[lcolnumbertype[i]] = df2[lcolnumbertype[i]].fillna(0)
                        else:
                            print('Fuera de If ')

                        # df2[lcolnumbertype[i]].astype(np.float).astype("Int32")
                        try:
                            df2[lcolnumbertype[i]].astype(int)
                        except:
                            print('Error de conversion 2')
                            # df2[lcolnumbertype[i]]=0
                print(df2)
                print('--------------- desde text_input1----')
                print('Dependencia_Respuesta= ', Dependencia_Respuesta)
                print('---------------')

                # print('r_car_cama_cun_distanc---->= ', df2['r_car_cama_cun_distanc'] )

                # print('r_car_18_mas_total---->= ', df2['r_car_18_mas_total'] )
                print(df2.dtypes)
                print(df2.eval(Dependencia_Respuesta).values[0])
                print('---------------')

                print('*' * 50)

                # a=1
                # b=1
                # c=2
                # print('a= ',a, ' b= ', b, ' c=', c)
                # if a > 2 and b>2 or c==2:
                #    print('Dentro de If ')
                # else:
                #    print('Dentro de else ')

                # if DependenciaSiNo == '' and Dependencia_Respuesta != '' \
                #        and ops[StringOperator](df[(df['q_'] == int(Dependencia))]['resp'].values[0] ,int(PreguntaObj)):

                if df2.eval(Dependencia_Respuesta).values[0]:
                    #number = st.number_input(q, step=1, min_value=0)
                    #st.write('Seleccionaste: ', number)
                    #df['resp'][x] = number
                    titles = st.text_input(q, key='text_input1' + str(page))
                    st.write('Seleccionaste:', titles)
                    df['resp'][x] = titles
    if tipo == 'hora':
        import operator
        ops = {"+": operator.add,
               "-": operator.sub,
               "<": operator.lt,
               "<=": operator.le,
               "==": operator.eq,
               "!=": operator.ne,
               ">": operator.gt,
               ">=": operator.ge
               }  # etc.


        if DependenciaSiNo == '' and Dependencia_Respuesta == '':
            print('----if')
            #genre = st.radio(q, (op))
            hora =st.time_input('Ingreso de Hora')
            st.write('Seleccionaste:', hora)
            df['resp'][x] = hora
        else:
            print('----else')
            # print('df[(df[q_] == Dependencia)][resp].values[0]= ', df[(df['q_'] == Dependencia)]['resp'].values[0])
            # print('str(DependenciaSiNo)= ', str(DependenciaSiNo))#
            # print('Dependencia_Respuesta =  ', Dependencia_Respuesta)
            try:
                if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(
                        DependenciaSiNo) and Dependencia_Respuesta == '':
                    print('entro a if ')
                    # genre = st.radio(q, (op))
                    hora = st.time_input('Ingreso de Hora')
                    st.write('Seleccionaste:', hora)
                    df['resp'][x] = hora
            except:
                print('Error en primer If ')

            if Dependencia_Respuesta != '':
                print('entro a else ')
                StringOperator = ''.join([i for i in Dependencia_Respuesta if not i.isdigit()])
                PreguntaObj = ''.join([i for i in Dependencia_Respuesta if i.isdigit()])
                print('StringOperator           = ', StringOperator)
                print('PreguntaObj              = ', PreguntaObj)
                import re
                a = ['<', '>', '=', '==', '<=', '>=']
                aal = ['and', 'or']
                a1 = re.split(' ', StringOperator)
                list_a = (aal)
                list_b = set(a1)

                # list_a=[[el] for el in a]
                # list_b=[[el] for el in a1]
                print('list_a= ', list_a)
                print('list_b= ', list_b)

                set2 = set(list_b)

                result = [x for x in list_a if x[0] in list_b]
                result2 = filter(lambda list_a: list_a[0] in list_b, list_a)
                result3 = [x for x in list_a if x in set2]
                print('result= ', result)
                print('result2= ', result2)
                print('result2= ', result3)

                print('Dependencia_Respuesta1    = ', Dependencia_Respuesta)

                DR = re.split('and|or', Dependencia_Respuesta)
                print('Dependencia_Respuesta2    = ', DR)
                DR_3 = [s.replace(" ", "") for s in DR]
                print('Dependencia_Respuesta3  DR_3  = ', DR_3)
                DR1 = re.split('<|>|=|==|>=|<=', Dependencia_Respuesta)
                print('Dependencia_Respuesta4   = ', DR1)

                indeces = [i for i, x in enumerate(Dependencia_Respuesta) if x in a]
                print('indeces= ', indeces)
                from itertools import count
                zipped = [(i, j) for i, j in zip(count(), a) if j == Dependencia_Respuesta]
                print('zipped= ', zipped)

                StringOperator = ''.join([i for i in Dependencia_Respuesta if not i.isalpha()])
                PreguntaObj = ''.join([i for i in Dependencia_Respuesta if i.isalpha()])
                print('StringOperator   isalpha         = ', StringOperator)
                print('PreguntaObj      isalpha         = ', PreguntaObj)

                StringOperator1 = ''.join([i for i in StringOperator if not i.isdigit()])
                PreguntaObj1 = ''.join([i for i in StringOperator if i.isdigit()])
                print('StringOperator1   isdigit         = ', StringOperator1)
                print('PreguntaObj1      isdigit         = ', PreguntaObj1)
                print('StringOperator1   isdigit  list        = ', list(StringOperator1))
                print('PreguntaObj1      isdigit  list        = ', list(PreguntaObj1))
                DR = [s.replace(" ", ",") for s in StringOperator1]
                print('StringOperator1   isdigit   2      = ', DR)
                DR3 = re.split(' ', StringOperator1)
                print('PreguntaObj1      DR3      = ', DR3)
                without_empty_strings = [string for string in DR3 if string != ""]
                print('without_empty_strings      DR3      = ', without_empty_strings)

                a = ['<', '>', '=', '==', '<=', '>=']

                for i in range(len(a)):
                    DR_3 = [s.replace(a[i], ' ') for s in DR_3]
                print('DR_3= ', DR_3)

                DR_3 = re.split(' ', ' '.join(DR_3))
                DR_3 = [string for string in DR_3 if string != ""]

                print('*' * 50)
                print('DR_3= ', DR_3)
                print('result3= ', result3)
                print('without_empty_strings = ', without_empty_strings)
                print('*' * 50)
                print(list(df.columns))
                df0 = df[['Vars', 'resp', 'tipo']]
                print(df0)

                print('*' * 100)
                lcolnumbertype = df0[df0.tipo == 'number_input']['Vars'].tolist()
                print('lcolnumbertype= ', lcolnumbertype)

                print('*' * 100)

                df1 = df[['Vars', 'resp']]
                df1 = df0[['Vars', 'resp']]

                df1 = df1.set_index('Vars')
                print('df1= ', df1)

                df2 = df1.T
                print('df2=', df2)
                for i in range(len(lcolnumbertype)):
                    print('i= ', i)
                    try:
                        # df2[lcolnumbertype[i]] = df2[lcolnumbertype[i]].fillna(0)
                        df2[lcolnumbertype[i]].astype('int')
                        # df2[lcolnumbertype[i]].astype(str).astype(float).astype(int)
                        # df2[lcolnumbertype[i]].astype('Int32')
                        # df2[lcolnumbertype[i]].astype(float).astype('Int64')
                        df2[lcolnumbertype[i]] = pd.to_numeric(df2[lcolnumbertype[i]], errors='coerce').astype('Int64')

                        # df2[lcolnumbertype[i]].astype(np.float).astype("Int32")
                    except:
                        print('Error de conversion ')
                        print('lcolnumbertype[i]= ', lcolnumbertype[i])
                        print('df.columns= ', df.columns)
                        print(df[['tipo', 'Vars']])
                        print('df2[lcolnumbertype[i]].values= ', df2[lcolnumbertype[i]].values[0])
                        print('_>', df[df.Vars == lcolnumbertype[i]]['tipo'].values[0])

                        print('i df2[lcolnumbertype[i]]=', df2[lcolnumbertype[i]])
                        print('f df2[lcolnumbertype[i]]=', str(df2[lcolnumbertype[i]].values[0]))
                        if str(df[df.Vars == lcolnumbertype[i]]['tipo'].values[0]) == 'number_input':
                            print('dentro de If ')
                            df2[lcolnumbertype[i]] = df2[lcolnumbertype[i]].fillna(0)
                        else:
                            print('Fuera de If ')

                        # df2[lcolnumbertype[i]].astype(np.float).astype("Int32")
                        try:
                            df2[lcolnumbertype[i]].astype(int)
                        except:
                            print('Error de conversion 2')
                            # df2[lcolnumbertype[i]]=0
                print(df2)
                print('--------------- desde text_input1----')
                print('Dependencia_Respuesta= ', Dependencia_Respuesta)
                print('---------------')

                # print('r_car_cama_cun_distanc---->= ', df2['r_car_cama_cun_distanc'] )

                # print('r_car_18_mas_total---->= ', df2['r_car_18_mas_total'] )
                print(df2.dtypes)
                print(df2.eval(Dependencia_Respuesta).values[0])
                print('---------------')

                print('*' * 50)

                # a=1
                # b=1
                # c=2
                # print('a= ',a, ' b= ', b, ' c=', c)
                # if a > 2 and b>2 or c==2:
                #    print('Dentro de If ')
                # else:
                #    print('Dentro de else ')

                # if DependenciaSiNo == '' and Dependencia_Respuesta != '' \
                #        and ops[StringOperator](df[(df['q_'] == int(Dependencia))]['resp'].values[0] ,int(PreguntaObj)):

                if df2.eval(Dependencia_Respuesta).values[0]:
                    #number = st.number_input(q, step=1, min_value=0)
                    #st.write('Seleccionaste: ', number)
                    #df['resp'][x] = number
                    # genre = st.radio(q, (op))
                    hora = st.time_input('Ingreso de Hora')
                    st.write('Seleccionaste:', hora)
                    df['resp'][x] = hora

    if tipo == 'radio':
        import operator
        ops = {"+": operator.add,
               "-": operator.sub,
               "<": operator.lt,
               "<=": operator.le,
               "==": operator.eq,
               "!=": operator.ne,
               ">": operator.gt,
               ">=": operator.ge
               }  # etc.


        if DependenciaSiNo == '' and Dependencia_Respuesta == '':
            print('----if')
            genre = st.radio(q, (op))
            st.write('Seleccionaste:', genre)
            df['resp'][x] = genre
        else:
            print('----else')
            # print('df[(df[q_] == Dependencia)][resp].values[0]= ', df[(df['q_'] == Dependencia)]['resp'].values[0])
            # print('str(DependenciaSiNo)= ', str(DependenciaSiNo))#
            # print('Dependencia_Respuesta =  ', Dependencia_Respuesta)
            try:
                if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(
                        DependenciaSiNo) and Dependencia_Respuesta == '':
                    print('entro a if ')
                    genre = st.radio(q, (op))
                    st.write('Seleccionaste:', genre)
                    df['resp'][x] = genre
            except:
                print('Error en primer If ')

            if Dependencia_Respuesta != '':
                print('entro a else ')
                StringOperator = ''.join([i for i in Dependencia_Respuesta if not i.isdigit()])
                PreguntaObj = ''.join([i for i in Dependencia_Respuesta if i.isdigit()])
                print('StringOperator           = ', StringOperator)
                print('PreguntaObj              = ', PreguntaObj)
                import re
                a = ['<', '>', '=', '==', '<=', '>=']
                aal = ['and', 'or']
                a1 = re.split(' ', StringOperator)
                list_a = (aal)
                list_b = set(a1)

                # list_a=[[el] for el in a]
                # list_b=[[el] for el in a1]
                print('list_a= ', list_a)
                print('list_b= ', list_b)

                set2 = set(list_b)

                result = [x for x in list_a if x[0] in list_b]
                result2 = filter(lambda list_a: list_a[0] in list_b, list_a)
                result3 = [x for x in list_a if x in set2]
                print('result= ', result)
                print('result2= ', result2)
                print('result2= ', result3)

                print('Dependencia_Respuesta1    = ', Dependencia_Respuesta)

                DR = re.split('and|or', Dependencia_Respuesta)
                print('Dependencia_Respuesta2    = ', DR)
                DR_3 = [s.replace(" ", "") for s in DR]
                print('Dependencia_Respuesta3  DR_3  = ', DR_3)
                DR1 = re.split('<|>|=|==|>=|<=', Dependencia_Respuesta)
                print('Dependencia_Respuesta4   = ', DR1)

                indeces = [i for i, x in enumerate(Dependencia_Respuesta) if x in a]
                print('indeces= ', indeces)
                from itertools import count
                zipped = [(i, j) for i, j in zip(count(), a) if j == Dependencia_Respuesta]
                print('zipped= ', zipped)

                StringOperator = ''.join([i for i in Dependencia_Respuesta if not i.isalpha()])
                PreguntaObj = ''.join([i for i in Dependencia_Respuesta if i.isalpha()])
                print('StringOperator   isalpha         = ', StringOperator)
                print('PreguntaObj      isalpha         = ', PreguntaObj)

                StringOperator1 = ''.join([i for i in StringOperator if not i.isdigit()])
                PreguntaObj1 = ''.join([i for i in StringOperator if i.isdigit()])
                print('StringOperator1   isdigit         = ', StringOperator1)
                print('PreguntaObj1      isdigit         = ', PreguntaObj1)
                print('StringOperator1   isdigit  list        = ', list(StringOperator1))
                print('PreguntaObj1      isdigit  list        = ', list(PreguntaObj1))
                DR = [s.replace(" ", ",") for s in StringOperator1]
                print('StringOperator1   isdigit   2      = ', DR)
                DR3 = re.split(' ', StringOperator1)
                print('PreguntaObj1      DR3      = ', DR3)
                without_empty_strings = [string for string in DR3 if string != ""]
                print('without_empty_strings      DR3      = ', without_empty_strings)

                a = ['<', '>', '=', '==', '<=', '>=']

                for i in range(len(a)):
                    DR_3 = [s.replace(a[i], ' ') for s in DR_3]
                print('DR_3= ', DR_3)

                DR_3 = re.split(' ', ' '.join(DR_3))
                DR_3 = [string for string in DR_3 if string != ""]

                print('*' * 50)
                print('DR_3= ', DR_3)
                print('result3= ', result3)
                print('without_empty_strings = ', without_empty_strings)
                print('*' * 50)
                print(list(df.columns))
                df0 = df[['Vars', 'resp', 'tipo']]
                print(df0)

                print('*' * 100)
                lcolnumbertype = df0[df0.tipo == 'number_input']['Vars'].tolist()
                print('lcolnumbertype= ', lcolnumbertype)

                print('*' * 100)

                df1 = df[['Vars', 'resp']]
                df1 = df0[['Vars', 'resp']]

                df1 = df1.set_index('Vars')
                print('df1= ', df1)

                df2 = df1.T
                print('df2=', df2)
                for i in range(len(lcolnumbertype)):
                    print('i= ', i)
                    try:
                        # df2[lcolnumbertype[i]] = df2[lcolnumbertype[i]].fillna(0)
                        df2[lcolnumbertype[i]].astype('int')
                        # df2[lcolnumbertype[i]].astype(str).astype(float).astype(int)
                        # df2[lcolnumbertype[i]].astype('Int32')
                        # df2[lcolnumbertype[i]].astype(float).astype('Int64')
                        df2[lcolnumbertype[i]] = pd.to_numeric(df2[lcolnumbertype[i]], errors='coerce').astype('Int64')

                        # df2[lcolnumbertype[i]].astype(np.float).astype("Int32")
                    except:
                        print('Error de conversion ')
                        print('lcolnumbertype[i]= ', lcolnumbertype[i])
                        print('df.columns= ', df.columns)
                        print(df[['tipo', 'Vars']])
                        print('df2[lcolnumbertype[i]].values= ', df2[lcolnumbertype[i]].values[0])
                        print('_>', df[df.Vars == lcolnumbertype[i]]['tipo'].values[0])

                        print('i df2[lcolnumbertype[i]]=', df2[lcolnumbertype[i]])
                        print('f df2[lcolnumbertype[i]]=', str(df2[lcolnumbertype[i]].values[0]))
                        if str(df[df.Vars == lcolnumbertype[i]]['tipo'].values[0]) == 'number_input':
                            print('dentro de If ')
                            df2[lcolnumbertype[i]] = df2[lcolnumbertype[i]].fillna(0)
                        else:
                            print('Fuera de If ')

                        # df2[lcolnumbertype[i]].astype(np.float).astype("Int32")
                        try:
                            df2[lcolnumbertype[i]].astype(int)
                        except:
                            print('Error de conversion 2')
                            # df2[lcolnumbertype[i]]=0
                print(df2)
                print('--------------- desde text_input1----')
                print('Dependencia_Respuesta= ', Dependencia_Respuesta)
                print('---------------')

                # print('r_car_cama_cun_distanc---->= ', df2['r_car_cama_cun_distanc'] )

                # print('r_car_18_mas_total---->= ', df2['r_car_18_mas_total'] )
                print(df2.dtypes)
                print(df2.eval(Dependencia_Respuesta).values[0])
                print('---------------')

                print('*' * 50)

                # a=1
                # b=1
                # c=2
                # print('a= ',a, ' b= ', b, ' c=', c)
                # if a > 2 and b>2 or c==2:
                #    print('Dentro de If ')
                # else:
                #    print('Dentro de else ')

                # if DependenciaSiNo == '' and Dependencia_Respuesta != '' \
                #        and ops[StringOperator](df[(df['q_'] == int(Dependencia))]['resp'].values[0] ,int(PreguntaObj)):

                if df2.eval(Dependencia_Respuesta).values[0]:
                    #number = st.number_input(q, step=1, min_value=0)
                    #st.write('Seleccionaste: ', number)
                    #df['resp'][x] = number
                    genre = st.radio(q, (op))
                    st.write('Seleccionaste:', genre)
                    df['resp'][x] = genre

    if tipo == 'date_input':
        if Dependencia == '':
            Fecha = st.date_input(q,
                                  min_value=dt.datetime.today() + dt.timedelta(days=-30),
                                  max_value=dt.datetime.today() + dt.timedelta(days=14))
            st.write('Fecha:', Fecha)
            # tr = datetime.strptime(dt.datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d').date()
            # tr = datetime.strptime(Fecha.strftime('%Y-%m-%d'), '%Y-%m-%d')

            df['resp'][x] = str(Fecha.strftime('%Y-%m-%d'))


        else:
            # print('--->', df[(df['q'] == qe)]['resp'].values[0])
            if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(DependenciaSiNo):
                Fecha = st.date_input(q,
                                      min_value=dt.datetime.today() + dt.timedelta(days=-30),
                                      max_value=dt.datetime.today() + dt.timedelta(days=14))
                st.write('Fecha:', Fecha)
                df['resp'][x] = str(Fecha.strftime('%Y-%m-%d'))
    if tipo == 'number_input':
        print('df= ', df)
        print('df q= ', df['q_'])
        print('Dependencia= ', Dependencia)
        print('DependenciaSiNo= ', DependenciaSiNo)
        print('Dependencia_Respuesta= ', Dependencia_Respuesta)

        import operator
        ops = {"+": operator.add,
               "-": operator.sub,
               "<": operator.lt,
               "<=": operator.le,
               "==": operator.eq,
               "!=": operator.ne,
               ">": operator.gt,
               ">=": operator.ge
               }  # etc.

        # if Dependencia == '' :
        if DependenciaSiNo == '' and Dependencia_Respuesta == '':
            print('qe vacio ')
            number = st.number_input(q, step=1, min_value=0)
            st.write('Seleccionaste: ', number)
            df['resp'][x] = str(int(number))
        else:

            # print('df[(df[q_] == Dependencia)][resp].values[0]= ', df[(df['q_'] == Dependencia)]['resp'].values[0])
            # print('str(DependenciaSiNo)= ', str(DependenciaSiNo))#
            # print('Dependencia_Respuesta =  ', Dependencia_Respuesta)
            try:
                if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(
                        DependenciaSiNo) and Dependencia_Respuesta == '':
                    print('entro a if ')
                    number = st.number_input(q, step=1, min_value=0)
                    st.write('Seleccionaste: ', number)
                    df['resp'][x] = str(int(number))
            except:
                print('Error en primer If ')

            if Dependencia_Respuesta != '':
                print('entro a else ')
                StringOperator = ''.join([i for i in Dependencia_Respuesta if not i.isdigit()])
                PreguntaObj = ''.join([i for i in Dependencia_Respuesta if i.isdigit()])
                print('StringOperator           = ', StringOperator)
                print('PreguntaObj              = ', PreguntaObj)
                import re
                a = ['<', '>', '=', '==', '<=', '>=']
                aal = ['and', 'or']
                a1 = re.split(' ', StringOperator)
                list_a = (aal)
                list_b = set(a1)

                # list_a=[[el] for el in a]
                # list_b=[[el] for el in a1]
                print('list_a= ', list_a)
                print('list_b= ', list_b)

                set2 = set(list_b)

                result = [x for x in list_a if x[0] in list_b]
                result2 = filter(lambda list_a: list_a[0] in list_b, list_a)
                result3 = [x for x in list_a if x in set2]
                print('result= ', result)
                print('result2= ', result2)
                print('result2= ', result3)

                print('Dependencia_Respuesta1    = ', Dependencia_Respuesta)

                DR = re.split('and|or', Dependencia_Respuesta)
                print('Dependencia_Respuesta2    = ', DR)
                DR_3 = [s.replace(" ", "") for s in DR]
                print('Dependencia_Respuesta3  DR_3  = ', DR_3)
                DR1 = re.split('<|>|=|==|>=|<=', Dependencia_Respuesta)
                print('Dependencia_Respuesta4   = ', DR1)

                indeces = [i for i, x in enumerate(Dependencia_Respuesta) if x in a]
                print('indeces= ', indeces)
                from itertools import count
                zipped = [(i, j) for i, j in zip(count(), a) if j == Dependencia_Respuesta]
                print('zipped= ', zipped)

                StringOperator = ''.join([i for i in Dependencia_Respuesta if not i.isalpha()])
                PreguntaObj = ''.join([i for i in Dependencia_Respuesta if i.isalpha()])
                print('StringOperator   isalpha         = ', StringOperator)
                print('PreguntaObj      isalpha         = ', PreguntaObj)

                StringOperator1 = ''.join([i for i in StringOperator if not i.isdigit()])
                PreguntaObj1 = ''.join([i for i in StringOperator if i.isdigit()])
                print('StringOperator1   isdigit         = ', StringOperator1)
                print('PreguntaObj1      isdigit         = ', PreguntaObj1)
                print('StringOperator1   isdigit  list        = ', list(StringOperator1))
                print('PreguntaObj1      isdigit  list        = ', list(PreguntaObj1))
                DR = [s.replace(" ", ",") for s in StringOperator1]
                print('StringOperator1   isdigit   2      = ', DR)
                DR3 = re.split(' ', StringOperator1)
                print('PreguntaObj1      DR3      = ', DR3)
                without_empty_strings = [string for string in DR3 if string != ""]
                print('without_empty_strings      DR3      = ', without_empty_strings)

                a = ['<', '>', '=', '==', '<=', '>=']

                for i in range(len(a)):
                    DR_3 = [s.replace(a[i], ' ') for s in DR_3]
                print('DR_3= ', DR_3)

                DR_3 = re.split(' ', ' '.join(DR_3))
                DR_3 = [string for string in DR_3 if string != ""]

                print('*' * 50)
                print('DR_3= ', DR_3)
                print('result3= ', result3)
                print('without_empty_strings = ', without_empty_strings)
                print('*' * 50)
                print(list(df.columns))
                df0 = df[['Vars', 'resp', 'tipo']]
                print(df0)

                print('*' * 100)
                lcolnumbertype = df0[df0.tipo == 'number_input']['Vars'].tolist()
                print('lcolnumbertype= ', lcolnumbertype)

                print('*' * 100)

                df1 = df[['Vars', 'resp']]
                df1 = df0[['Vars', 'resp']]

                df1 = df1.set_index('Vars')
                print('df1= ', df1)

                df2 = df1.T
                print('df2=', df2)
                for i in range(len(lcolnumbertype)):
                    print('i= ', i)
                    try:
                        # df2[lcolnumbertype[i]] = df2[lcolnumbertype[i]].fillna(0)
                        df2[lcolnumbertype[i]].astype('int')
                        # df2[lcolnumbertype[i]].astype(str).astype(float).astype(int)
                        # df2[lcolnumbertype[i]].astype('Int32')
                        # df2[lcolnumbertype[i]].astype(float).astype('Int64')
                        df2[lcolnumbertype[i]] = pd.to_numeric(df2[lcolnumbertype[i]], errors='coerce').astype('Int64')

                        # df2[lcolnumbertype[i]].astype(np.float).astype("Int32")
                    except:
                        print('Error de conversion ')
                        print('lcolnumbertype[i]= ', lcolnumbertype[i])
                        print('df.columns= ', df.columns)
                        print(df[['tipo', 'Vars']])
                        print('df2[lcolnumbertype[i]].values= ', df2[lcolnumbertype[i]].values[0])
                        print('_>', df[df.Vars == lcolnumbertype[i]]['tipo'].values[0])

                        print('i df2[lcolnumbertype[i]]=', df2[lcolnumbertype[i]])
                        print('f df2[lcolnumbertype[i]]=', str(df2[lcolnumbertype[i]].values[0]))
                        if str(df[df.Vars == lcolnumbertype[i]]['tipo'].values[0]) == 'number_input':
                            print('dentro de If ')
                            df2[lcolnumbertype[i]] = df2[lcolnumbertype[i]].fillna(0)
                        else:
                            print('Fuera de If ')

                        # df2[lcolnumbertype[i]].astype(np.float).astype("Int32")
                        try:
                            df2[lcolnumbertype[i]].astype(int)
                        except:
                            print('Error de conversion 2')
                            # df2[lcolnumbertype[i]]=0
                print(df2)
                print('--------------- desde number_input----')
                print('Dependencia_Respuesta= ', Dependencia_Respuesta)
                print('---------------')

                # print('r_car_cama_cun_distanc---->= ', df2['r_car_cama_cun_distanc'] )

                # print('r_car_18_mas_total---->= ', df2['r_car_18_mas_total'] )
                print(df2.dtypes)
                print(df2.eval(Dependencia_Respuesta).values[0])
                print('---------------')

                print('*' * 50)

                # a=1
                # b=1
                # c=2
                # print('a= ',a, ' b= ', b, ' c=', c)
                # if a > 2 and b>2 or c==2:
                #    print('Dentro de If ')
                # else:
                #    print('Dentro de else ')

                # if DependenciaSiNo == '' and Dependencia_Respuesta != '' \
                #        and ops[StringOperator](df[(df['q_'] == int(Dependencia))]['resp'].values[0] ,int(PreguntaObj)):
                if df2.eval(Dependencia_Respuesta).values[0]:
                    number = st.number_input(q, step=1, min_value=0)
                    st.write('Seleccionaste: ', number)
                    df['resp'][x] = number

        if Validar != '':
            temp_string = Validar
            s = Validar

            StringOperator = ''.join([i for i in s if not i.isdigit()])
            PreguntaObj = ''.join([i for i in s if i.isdigit()])

            # st.write('temp_string: ', temp_string)
            # st.write('StringOperator: ', StringOperator)
            # st.write('PreguntaObj: ', PreguntaObj)

            # st.write('PreguntaObj PreguntaObj   : ', df[(df['q_'] == int(PreguntaObj))]['resp'].values[0])
            # st.write(' x=',x,' q=', q,' op=', op,' qe=', qe)

            # st.write('PreguntaObj x            : ', df[(df['q_'] == int(x))]['resp'].values[0])
            # st.write('Operador r3: ',StringOperator ,' ---- ',  ops[StringOperator](df[(df['q_'] == int(PreguntaObj))]['resp'].values[0], df[(df['q_'] == int(x))]['resp'].values[0]))

            if ops[StringOperator](df[(df['q_'] == int(x))]['resp'].values[0],
                                   df[(df['q_'] == int(PreguntaObj))]['resp'].values[0]):
                print('ok')
            else:
                st.error('Error de Validacion. La pregunta ' + str(x) + ' debe ser ' + str(
                    StringOperator) + ' que la pregunta ' + str(PreguntaObj))

        # print(ops["+"](1, 1))  # prints 2
        # st.write('result3: ', ops["+"](1, 1))

        # print('tipo  number_input')
        # print(df)
    if tipo == 'number_input%':
        if Dependencia == '':
            number = st.number_input(q, max_value=100)
            st.write('Seleccionaste: ', number, ' %')
            df['resp'][x] = number
        else:
            # print('--->', df[ (df['q'] == qe)]['resp'].values[0])
            if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(DependenciaSiNo):
                number = st.number_input(q, max_value=100)
                st.write('Seleccionaste: ', number, ' %')
                df['resp'][x] = number
    if tipo == 'text_input_Multiple':
        if Dependencia == '':
            st.write(q)
            a = []
            for i in range(len(op)):
                title1 = st.text_input(op[i], key=str(i))
                a.append(title1)
            st.write('Seleccionaste:', a)
            df['resp'][x] = a
        else:
            if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(DependenciaSiNo):
                # title1 = st.text_input(q , key='1')
                st.write(q)
                a = []
                for i in range(len(op)):
                    title1 = st.text_input(op[i], key=str(i))
                    a.append(title1)
                st.write('Seleccionaste:', a)
                df['resp'][x] = a
    if tipo == 'number_input_Multiple':
        if Dependencia == '':
            st.write(q)
            a = []
            for i in range(len(op)):
                # number = st.number_input(q, <<<step=1<<<)
                title1 = st.number_input(op[i], step=1, key=(str(i) + str(x)))
                a.append(title1)
            st.write('Seleccionaste:', a)
            df['resp'][x] = str(a)
        else:
            if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(DependenciaSiNo):
                # title1 = st.text_input(q , key='1')
                st.write(q)
                a = []
                for i in range(len(op)):
                    title1 = st.number_input(op[i], step=1, key=(str(i) + str(x)))
                    a.append(title1)
                st.write('Seleccionaste:', a)
                df['resp'][x] = str(a)

    if tipo == 'Ipress_Metadata':
        DFMetadata = CargaMetadata('Ipress_Metadata')
        if vista != 'No':
            print('Ipress_Metadata-================')
            print('op= ', op)

            # DFMetadata=CargaMetadata('Ipress_Metadata')
            dflocal = DFMetadata
            # print(dflocal.head())
            t = st.session_state
            print('st.session_state= ', t)
            result = [x for x in t if x.startswith('Metadata_')]
            # result= result.remove('Tipo')
            print('result1= ', result)
            # result = [x for x in result if x is not ['Metadata_Código Único',
            #                                         'Metadata_Nombre del establecimiento']]
            # l2=['Metadata_Código Único','Metadata_Nombre del establecimiento','Metadata_Departamento']
            # resultr = [x for x in result if x not in l2]
            # result.remove('Metadata_Código Único') if '' in s else None
            # result.remove('Metadata_Nombre del establecimiento') if '' in s else None
            # result.remove('Metadata_Departamento') if '' in s else None
            # result=resultr
            # print('result r= ', resultr)

            # nivel=[1,2,3,4]

            print('===========================filtros=======================================')

            print('dflocal cols', df.columns)
            print('actual   = ', op[0])
            print(df['nivel'].unique())
            dfq = df[(df['tipo'] == tipo)]
            print(dfq['nivel'].unique())

            print('Afectado = ', dfq[(dfq['nivel'].astype(int) < int(nivel))]['op'].tolist())
            lk = dfq[(dfq['nivel'].astype(int) < int(nivel))]['op'].tolist()

            for ir in lk:
                print('---->', ir)
                if op[0] != 'Departamento':
                    dflocal = dflocal[(dflocal[ir[0]] == st.session_state['Metadata_' + ir[0]])]

            print('===========================filtros=======================================')

            print('result== ', result)
            print(dflocal[['Código Único', 'Nombre del establecimiento',
                           'Departamento', 'Provincia', 'Distrito']])
            print('===========================uniques=======================================')
            print('dflocal Departamento ', dflocal['Departamento'].unique())
            print('dflocal Provincia    ', dflocal['Provincia'].unique())
            print('dflocal Distrito     ', dflocal['Distrito'].unique())

            print('===========================uniques=======================================')

            if Dependencia == '':
                optionMetadata = st.selectbox(q, dflocal[op[0]].unique().tolist(), key=('Metadata_' + op[0]))
                st.write('Seleccionaste:', optionMetadata)
                df['resp'][x] = str(optionMetadata)
                print('optionMetadata= ', optionMetadata)

                # print('optionMetadata=', st.session_state[op])
            else:
                if df[(df['q_'] == Dependencia)]['resp'].values[0] == 'Si':
                    optionMetadata = st.selectbox(q, dflocal[op[0]].unique().tolist(), key=('Metadata_' + op[0]))
                    st.write('Seleccionaste:', optionMetadata)
                    df['resp'][x] = str(optionMetadata)
                    print('optionMetadata= ', optionMetadata)
        else:
            print('colum', df.columns)
            dff = df
            print('-------------------------------------')
            dff1 = df[(df['tipo'] == tipo) & (df['Vista'] == 'No')]
            print('Max1= ', dff1['nivel'].max())
            print(dff1)
            print(dff1['op'].values[0][0])
            print('-------------------------------------')
            dff2 = df[(df['tipo'] == tipo) & (df['Vista'] != 'No')]
            print('Max2= ', dff2['nivel'].max())
            print(dff2)
            print(dff2[(dff2['nivel'] == dff1['nivel'].max())])
            tt = dff2[(dff2['nivel'] == dff1['nivel'].max())]
            print(tt)
            print('--1', tt['op'].values[0][0])
            print('--2', tt['resp'].values[0])
            print(DFMetadata[(DFMetadata[tt['op'].values[0][0]] == tt['resp'].values[0])][tt['op'].values[0][0]].values[
                      0])
            print('-------------------------------------3')

            df['resp'][x] = \
                DFMetadata[(DFMetadata[tt['op'].values[0][0]] == tt['resp'].values[0])][tt['op'].values[0][0]].values[0]
    if tipo == 'Comisarias_Metadata':
        DFMetadata = CargaMetadata('Comisarias_Metadata')
        if vista != 'No':
            print('Comisarias_Metadata-================')
            print('op= ', op)

            # DFMetadata=CargaMetadata('Ipress_Metadata')
            dflocal = DFMetadata
            # print(dflocal.head())
            t = st.session_state
            print('st.session_state= ', t)
            result = [x for x in t if x.startswith('Metadata_')]
            # result= result.remove('Tipo')
            print('result1= ', result)
            # result = [x for x in result if x is not ['Metadata_Código Único',
            #                                         'Metadata_Nombre del establecimiento']]
            # l2=['Metadata_Código Único','Metadata_Nombre del establecimiento','Metadata_Departamento']
            # resultr = [x for x in result if x not in l2]
            # result.remove('Metadata_Código Único') if '' in s else None
            # result.remove('Metadata_Nombre del establecimiento') if '' in s else None
            # result.remove('Metadata_Departamento') if '' in s else None
            # result=resultr
            # print('result r= ', resultr)

            # nivel=[1,2,3,4]

            print('===========================filtros=======================================')

            print('dflocal cols', df.columns)
            print('actual   = ', op[0])
            print(df['nivel'].unique())
            dfq = df[(df['tipo'] == tipo)]
            print(dfq['nivel'].unique())

            print('Afectado = ', dfq[(dfq['nivel'].astype(int) < int(nivel))]['op'].tolist())
            lk = dfq[(dfq['nivel'].astype(int) < int(nivel))]['op'].tolist()

            for ir in lk:
                print('---->', ir)
                if op[0] != 'Departamento':
                    dflocal = dflocal[(dflocal[ir[0]] == st.session_state['Metadata_' + ir[0]])]

            print('===========================filtros=======================================')

            print('result== ', result)
            print(dflocal[['Código Único', 'Nombre del establecimiento',
                           'Departamento', 'Provincia', 'Distrito']])
            print('===========================uniques=======================================')
            print('dflocal Departamento ', dflocal['Departamento'].unique())
            print('dflocal Provincia    ', dflocal['Provincia'].unique())
            print('dflocal Distrito     ', dflocal['Distrito'].unique())

            print('===========================uniques=======================================')

            if Dependencia == '':
                optionMetadata = st.selectbox(q, dflocal[op[0]].unique().tolist(), key=('Metadata_' + op[0]))
                st.write('Seleccionaste:', optionMetadata)
                df['resp'][x] = str(optionMetadata)
                print('optionMetadata= ', optionMetadata)

                # print('optionMetadata=', st.session_state[op])
            else:
                if df[(df['q_'] == Dependencia)]['resp'].values[0] == 'Si':
                    optionMetadata = st.selectbox(q, dflocal[op[0]].unique().tolist(), key=('Metadata_' + op[0]))
                    st.write('Seleccionaste:', optionMetadata)
                    df['resp'][x] = str(optionMetadata)
                    print('optionMetadata= ', optionMetadata)
        else:
            print('colum', df.columns)
            dff = df
            print('-------------------------------------')
            dff1 = df[(df['tipo'] == tipo) & (df['Vista'] == 'No')]
            print('Max1= ', dff1['nivel'].max())
            print(dff1)
            print(dff1['op'].values[0][0])
            print('-------------------------------------')
            dff2 = df[(df['tipo'] == tipo) & (df['Vista'] != 'No')]
            print('Max2= ', dff2['nivel'].max())
            print(dff2)
            print(dff2[(dff2['nivel'] == dff1['nivel'].max())])
            tt = dff2[(dff2['nivel'] == dff1['nivel'].max())]
            print(tt)
            print('--1', tt['op'].values[0][0])
            print('--2', tt['resp'].values[0])
            print(DFMetadata[(DFMetadata[tt['op'].values[0][0]] == tt['resp'].values[0])][tt['op'].values[0][0]].values[
                      0])
            print('-------------------------------------3')

            df['resp'][x] = \
                DFMetadata[(DFMetadata[tt['op'].values[0][0]] == tt['resp'].values[0])][tt['op'].values[0][0]].values[0]

    if tipo == 'Car_Metadata':
        DFMetadata = CargaMetadata('Car_Metadata')
        if vista != 'No':
            print('Comisarias_Metadata-================')
            print('op= ', op)

            # DFMetadata=CargaMetadata('Ipress_Metadata')
            dflocal = DFMetadata
            # print(dflocal.head())
            t = st.session_state
            print('st.session_state= ', t)
            result = [x for x in t if x.startswith('Metadata_')]
            # result= result.remove('Tipo')
            print('result1= ', result)
            # result = [x for x in result if x is not ['Metadata_Código Único',
            #                                         'Metadata_Nombre del establecimiento']]
            # l2=['Metadata_Código Único','Metadata_Nombre del establecimiento','Metadata_Departamento']
            # resultr = [x for x in result if x not in l2]
            # result.remove('Metadata_Código Único') if '' in s else None
            # result.remove('Metadata_Nombre del establecimiento') if '' in s else None
            # result.remove('Metadata_Departamento') if '' in s else None
            # result=resultr
            # print('result r= ', resultr)

            # nivel=[1,2,3,4]

            print('===========================filtros=======================================')

            print(' df', df)
            print('dflocal cols', df.columns)
            print('actual   = ', op[0])
            print(df['nivel'].unique())
            dfq = df[(df['tipo'] == tipo)]
            print(dfq['nivel'].unique())
            print('--' * 50)
            print('nivel= ', nivel)

            print('dfq= ', dfq)
            print('dfq columns= ', dfq.columns)
            print('dfq[nivel]= ', dfq['nivel'])
            print('dfq[op]= ', dfq['op'])

            # print('Afectado = ', dfq[(dfq['nivel'].astype(int) < int(nivel))]['op'].tolist())
            lk = dfq[(dfq['nivel'].astype(int) < int(nivel))]['op'].tolist()

            for ir in lk:
                print('---->', ir)
                if op[0] != 'Departamento':
                    dflocal = dflocal[(dflocal[ir[0]] == st.session_state['Metadata_' + ir[0]])]

            print('===========================filtros=======================================')

            print('result== ', result)
            print(dflocal[['Código Único', 'Nombre del establecimiento',
                           'Departamento', 'Provincia', 'Distrito']])
            print('===========================uniques=======================================')
            print('dflocal Departamento ', dflocal['Departamento'].unique())
            print('dflocal Provincia    ', dflocal['Provincia'].unique())
            print('dflocal Distrito     ', dflocal['Distrito'].unique())

            print('===========================uniques=======================================')

            if Dependencia == '':
                optionMetadata = st.selectbox(q, dflocal[op[0]].unique().tolist(), key=('Metadata_' + op[0]))
                st.write('Seleccionaste:', optionMetadata)
                df['resp'][x] = str(optionMetadata)
                print('optionMetadata= ', optionMetadata)

                # print('optionMetadata=', st.session_state[op])
            else:
                if df[(df['q_'] == Dependencia)]['resp'].values[0] == 'Si':
                    optionMetadata = st.selectbox(q, dflocal[op[0]].unique().tolist(), key=('Metadata_' + op[0]))
                    st.write('Seleccionaste:', optionMetadata)
                    df['resp'][x] = str(optionMetadata)
                    print('optionMetadata= ', optionMetadata)
        else:
            print('colum', df.columns)
            dff = df
            print('-------------------------------------')
            dff1 = df[(df['tipo'] == tipo) & (df['Vista'] == 'No')]
            print('Max1= ', dff1['nivel'].max())
            print(dff1)
            print(dff1['op'].values[0][0])
            print('-------------------------------------')
            dff2 = df[(df['tipo'] == tipo) & (df['Vista'] != 'No')]
            print('Max2= ', dff2['nivel'].max())
            print(dff2)
            print(dff2[(dff2['nivel'] == dff1['nivel'].max())])
            tt = dff2[(dff2['nivel'] == dff1['nivel'].max())]
            print(tt)
            print('--1', tt['op'].values[0][0])
            print('--2', tt['resp'].values[0])
            print(DFMetadata[(DFMetadata[tt['op'].values[0][0]] == tt['resp'].values[0])][tt['op'].values[0][0]].values[
                      0])
            print('-------------------------------------3')

            df['resp'][x] = \
                DFMetadata[(DFMetadata[tt['op'].values[0][0]] == tt['resp'].values[0])][tt['op'].values[0][0]].values[0]

            print('df[resp][x] = ', df['resp'][x])

    if tipo == 'foto':
        st.write('1folder ---> ', os.getcwd())
        st.write('1folder 2---> ', os.chdir(os.getcwd()))

        # ['/app/icon-32x32.png', '/app/images/32x32.png']
        uploaded_file = '/app/icon-32x32.png'
        image = Image.open(uploaded_file)
        st.image(image, caption='Uploaded Image.', use_column_width=True)
        uploaded_file = '/app/images/32x32.png'
        image = Image.open(uploaded_file)
        st.image(image, caption='Uploaded Image.', use_column_width=True)
        print('-----------------------------------------------------1')
        print("Path at terminal when executing this file")
        print(os.getcwd() + "\n")
        print("This file path, relative to os.getcwd()")
        print(__file__ + "\n")
        print("This file full path (following symlinks)")
        full_path = os.path.realpath(__file__)
        print(full_path + "\n")
        print("This file directory and name")
        path, filename = os.path.split(full_path)
        print(path + ' --> ' + filename + "\n")
        print("This file directory only")
        print(os.path.dirname(full_path))
        print('busqueda i')
        for path, subdirs, files in os.walk(os.getcwd()):
            for name in files:
                we = os.path.join(path, name)
                # print('2=-->', we)
                name = we.split('/')[-1]
                # print('2name=-->', name)
                if (name == 'CAP.png'):
                    print('Bingo')
                # else:
                #    print('No esta causa')
        print('busqueda f')
        path = os.getcwd()

        text_files = glob.glob(path + "/**/*.png", recursive=True)

        print(text_files)
        print('busqueda f2')

        uploaded_file = st.file_uploader("Escoge las fotos a cargar: ", accept_multiple_files=True, type=['png', 'jpg'])
        print('-----------------------------------------------------2')
        print("Path at terminal when executing this file")
        print(os.getcwd() + "\n")
        print("This file path, relative to os.getcwd()")
        print(__file__ + "\n")
        print("This file full path (following symlinks)")
        full_path = os.path.realpath(__file__)
        print(full_path + "\n")
        print("This file directory and name")
        path, filename = os.path.split(full_path)
        print(path + ' --> ' + filename + "\n")
        print("This file directory only")
        print(os.path.dirname(full_path))
        print('busqueda i')
        for path, subdirs, files in os.walk(os.getcwd()):
            for name in files:
                we = os.path.join(path, name)
                # print('2=-->', we)
                name = we.split('/')[-1]
                # print('2name=-->', name)
                if (name == 'CAP.png'):
                    print('Bingo')
                # else:
                #    print('No esta causa')
        print('busqueda f')
        path = os.getcwd()

        text_files = glob.glob(path + "/**/*.png", recursive=True)

        print(text_files)
        print('busqueda f2')

        st.write('2folder ---> ', os.getcwd())
        st.write('2folder 2---> ', os.chdir(os.getcwd()))

        if uploaded_file is not None:
            st.write('---> ', uploaded_file)
            uploaded_file = '/app/CAP.png'
            image = Image.open(uploaded_file)
            st.image(image, caption='Uploaded Image.', use_column_width=True)
            st.write(".........")

        # To read file as string:
        # string_data = stringio.read()
        # st.write(string_data)
        # Can be used wherever a "file-like" object is accepted:
        # if uploaded_file is not None:
        # dataframe = pd.read_csv(uploaded_file)
        st.write('Existe folder con el nombre del formulario?')
        obj = DriveAPI()
        st.write(uploaded_file)
        folder_id = '1yhcPImMAu8AS93vj1PEByd5xYUEk94QE'
        t = obj.FolderSearch2(folder_id)
        st.write(t)
        df_t = pd.DataFrame(t)
        # global i
        st.write(df_t[(df_t['mimeType'] == 'application/vnd.google-apps.folder') & (df_t['name'] == str(page))])
        st.write(page)
        d = df_t[(df_t['mimeType'] == 'application/vnd.google-apps.folder') & (df_t['name'] == str(page))]
        if (len(d) == 0):
            st.write('len   0  Crear carpeta y guardar alli ')

            f = obj.FolderCreator(str(page), folder_id)
            st.write(str(f))
            st.write('id= ', f[1])
            name2 = 'Foto1'
            filepath = uploaded_file
            st.write('filepath= ', filepath)
            spl = str(filepath).split("'")

            st.write('filepath-spl= ', spl[1])
            st.write('uploaded_file= ', uploaded_file)

            # image = Image.open(uploaded_file)
            # st.image(image, caption='Sunrise by the mountains')

            r = obj.FileUpload_(uploaded_file, spl[1], f[1])
            st.write('r= ', r)

        else:
            st.write('len != 0 No crear y guardar en carpeta')
            st.write('id= ', d['id'].values[0])
            name2 = 'Foto1'
            filepath = uploaded_file
            st.write('filepath= ', filepath)
            spl = str(filepath).split("'")
            st.write('filepath-split= ', spl[1])
            st.write('uploaded_file= ', uploaded_file)

            # image = Image.open(uploaded_file)
            # st.image(image, caption='Sunrise by the mountains')

            r = obj.FileUpload_(uploaded_file, spl[1], d['id'].values[0])
            st.write('r= ', r)
        # st.write(uploaded_file)
    if tipo == 'number_input_Multiple_Comisarias':
        print('number_input_Multiple_Comisarias  ===========================================================')

        if Dependencia == '':
            st.write(q)
            a = []
            for i in range(len(op)):
                # number = st.number_input(q, <<<step=1<<<)
                title1 = st.number_input(op[i], step=1, key=(str(i) + str(x)))
                a.append(title1)

            st.write('Seleccionaste:', a)
            # df['resp'][x] = str(a)
            df['resp'][x] = str([",".join("{0}:{1}".format(x, y) for x, y in zip(op, a))])

            print(str(df[(df.index == int(x))]))
            df = df.append([df[(df.index == int(x))]], ignore_index=True)
            df = df.append([df[(df.index == int(x))]], ignore_index=True)

            print('len(df)= ', len(df))
            print('df.columns= ', df.columns)
            df['Vars'][(len(df) - 2)] = str(df['Vars'][x]) + ('_salida_2_s')
            df['resp'][(len(df) - 2)] = str(sum(a))

            df['Vars'][(len(df) - 1)] = str(df['Vars'][x]) + ('_salida_3_recuento')
            df['resp'][(len(df) - 1)] = str(len(a) - a.count(0))

            # test = df[['Vars','resp']].astype(str)
            # print(test)


        else:
            if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(DependenciaSiNo):
                # title1 = st.text_input(q , key='1')
                st.write(q)
                a = []
                for i in range(len(op)):
                    title1 = st.number_input(op[i], step=1, key=(str(i) + str(x)))
                    a.append(title1)
                st.write('Seleccionaste:', a)
                # df['resp'][x] = str(a)
                df['resp'][x] = str([",".join("{0}:{1}".format(x, y) for x, y in zip(op, a))])

                print(str(df[(df.index == int(x))]))
                df = df.append([df[(df.index == int(x))]], ignore_index=True)
                print('len(df)= ', len(df))
                print('df.columns= ', df.columns)
                df['Vars'][(len(df) - 1)] = str(df['Vars'][x]) + ('_salida_2')
                df['resp'][(len(df) - 1)] = str(sum(a))
                test = df[['Vars', 'resp']].astype(str)
                print(test)

        print('number_input_Multiple_Comisarias  ===========================================================')


def expanderrr2(x, q, op, tipo, Dependencia, nivel, vista, DependenciaSiNo, Validar,Dependencia_Respuesta):
    print('=============================================================')
    print('X= ', x)
    print('q= ', q)
    print('nivel= ', nivel)
    print('op= ', op)

    #print('Dependencia= |', Dependencia,'|', len(Dependencia))
    #print('DependenciaSiNo= |', DependenciaSiNo,'|', len(Dependencia))

    global df
    if tipo == 'selectbox':
        if Dependencia == '':
            option = st.selectbox(q, op)
            st.write('Seleccionaste:', option)
            df['resp'][x] = option
        else:
            if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(DependenciaSiNo):
                option = st.selectbox(q, op)
                st.write('Seleccionaste:', option)
                df['resp'][x] = option
    if tipo == 'multiselect':
        if Dependencia == '':
            options = st.multiselect(q, op)
            st.write('Seleccionaste:', options)
            df['resp'][x] = str(options)
        else:
            if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(DependenciaSiNo):
                options = st.multiselect(q, op)
                st.write('Seleccionaste:', options)
                df['resp'][x] = str(options)
    if tipo == 'text_input':
        import operator
        ops = {"+": operator.add,
               "-": operator.sub,
               "<": operator.lt,
               "<=": operator.le,
               "==": operator.eq,
               "!=": operator.ne,
               ">": operator.gt,
               ">=": operator.ge
               }  # etc.

        if Dependencia == '':
            titles = st.text_input(q, key='text_input1' + str(page))
            st.write('Seleccionaste:', titles)
            df['resp'][x] = titles
        else:
            if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(DependenciaSiNo):
                titles = st.text_input(q, key='text_input2' + str(page))
                st.write('Seleccionaste:', titles)
                df['resp'][x] = titles

        df0 = df[['Vars', 'resp', 'tipo']]
        print(df0)

        print('*' * 100)
        lcolnumbertype = df0[df0.tipo == 'number_input']['Vars'].tolist()
        print('lcolnumbertype= ', lcolnumbertype)

        print('*' * 100)

        df1 = df[['Vars', 'resp']]
        df1 = df0[['Vars', 'resp']]

        df1 = df1.set_index('Vars')
        print('df1= ', df1)

        df2 = df1.T
        print('df2=', df2)
        for i in range(len(lcolnumbertype)):
            print('i= ', i)
            try:
                # df2[lcolnumbertype[i]] = df2[lcolnumbertype[i]].fillna(0)
                df2[lcolnumbertype[i]].astype('int')
                # df2[lcolnumbertype[i]].astype(str).astype(float).astype(int)
                # df2[lcolnumbertype[i]].astype('Int32')
                # df2[lcolnumbertype[i]].astype(float).astype('Int64')
                df2[lcolnumbertype[i]] = pd.to_numeric(df2[lcolnumbertype[i]], errors='coerce').astype('Int64')

                # df2[lcolnumbertype[i]].astype(np.float).astype("Int32")
            except:
                print('Error de conversion ')
                print('lcolnumbertype[i]= ', lcolnumbertype[i])
                print('df.columns= ', df.columns)
                print(df[['tipo', 'Vars']])
                print('df2[lcolnumbertype[i]].values= ', df2[lcolnumbertype[i]].values[0])
                print('_>', df[df.Vars == lcolnumbertype[i]]['tipo'].values[0])

                print('i df2[lcolnumbertype[i]]=', df2[lcolnumbertype[i]])
                print('f df2[lcolnumbertype[i]]=', str(df2[lcolnumbertype[i]].values[0]))
                if str(df[df.Vars == lcolnumbertype[i]]['tipo'].values[0]) == 'number_input':
                    print('dentro de If ')
                    df2[lcolnumbertype[i]] = df2[lcolnumbertype[i]].fillna(0)
                else:
                    print('Fuera de If ')

                try:
                    df2[lcolnumbertype[i]].astype(int)
                except:
                    print('Error de conversion 2')
                    # df2[lcolnumbertype[i]]=0
        print(df2)
        print('---------------')
        print('Dependencia_Respuesta= ', Dependencia_Respuesta)
        # print('r_car_cama_cun_distanc---->= ', df2['r_car_cama_cun_distanc'] )

        # print('r_car_18_mas_total---->= ', df2['r_car_18_mas_total'] )
        print(df2.dtypes)
        print(df2.eval(Dependencia_Respuesta).values[0])
        if df2.eval(Dependencia_Respuesta).values[0]:
            number = st.number_input(q, step=1, min_value=0)
            st.write('Seleccionaste: ', number)
            df['resp'][x] = number


    if tipo == 'radio':
        if Dependencia == '':
            genre = st.radio(q, (op))
            st.write('Seleccionaste:', genre)
            df['resp'][x] = genre
        else:
            # print('--->', df[(df['q'] == qe)]['resp'].values[0])
            if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(DependenciaSiNo):
                genre = st.radio(q, (op))
                st.write('Seleccionaste:', genre)
                df['resp'][x] = genre
    if tipo == 'date_input':
        if Dependencia == '':
            Fecha = st.date_input(q,
                                  min_value=dt.datetime.today() + dt.timedelta(days=-30),
                                  max_value=dt.datetime.today() + dt.timedelta(days=14))
            st.write('Fecha:', Fecha)
            # tr = datetime.strptime(dt.datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d').date()
            # tr = datetime.strptime(Fecha.strftime('%Y-%m-%d'), '%Y-%m-%d')

            df['resp'][x] = str(Fecha.strftime('%Y-%m-%d'))


        else:
            # print('--->', df[(df['q'] == qe)]['resp'].values[0])
            if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(DependenciaSiNo):
                Fecha = st.date_input(q,
                                      min_value=dt.datetime.today() + dt.timedelta(days=-30),
                                      max_value=dt.datetime.today() + dt.timedelta(days=14))
                st.write('Fecha:', Fecha)
                df['resp'][x] = str(Fecha.strftime('%Y-%m-%d'))
    if tipo == 'number_input':
        print('df= ', df)
        print('df q= ',df['q_'] )
        print('Dependencia= ', Dependencia)
        print('DependenciaSiNo= ', DependenciaSiNo)
        print('Dependencia_Respuesta= ', Dependencia_Respuesta)

        import operator
        ops = {"+": operator.add,
               "-": operator.sub,
               "<": operator.lt,
               "<=": operator.le,
               "==": operator.eq,
               "!=": operator.ne,
               ">": operator.gt,
               ">=": operator.ge
               }  # etc.

        #if Dependencia == '' :
        if DependenciaSiNo == '' and Dependencia_Respuesta=='':
            print('qe vacio ')
            number = st.number_input(q, step=1, min_value=0)
            st.write('Seleccionaste: ', number)
            df['resp'][x] = str(int(number))
        else:


            #print('df[(df[q_] == Dependencia)][resp].values[0]= ', df[(df['q_'] == Dependencia)]['resp'].values[0])
            #print('str(DependenciaSiNo)= ', str(DependenciaSiNo))#
            #print('Dependencia_Respuesta =  ', Dependencia_Respuesta)
            try:
                if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(DependenciaSiNo) and Dependencia_Respuesta == '':
                    print('entro a if ')
                    number = st.number_input(q, step=1, min_value=0)
                    st.write('Seleccionaste: ', number)
                    df['resp'][x] = number
            except:
                print('Error en primer If ')

            if Dependencia_Respuesta != '':
                print('entro a else ')
                StringOperator = ''.join([i for i in Dependencia_Respuesta if not i.isdigit()])
                PreguntaObj = ''.join([i for i in Dependencia_Respuesta if i.isdigit()])
                print('StringOperator           = ', StringOperator)
                print('PreguntaObj              = ', PreguntaObj)
                import re
                a=['<', '>', '=', '==', '<=','>=']
                aal=['and', 'or']
                a1= re.split(' ', StringOperator)
                list_a=(aal)
                list_b=set(a1)

                #list_a=[[el] for el in a]
                #list_b=[[el] for el in a1]
                print('list_a= ',list_a)
                print('list_b= ',list_b)

                set2 = set(list_b)

                result = [x for x in list_a if x[0] in list_b]
                result2= filter(lambda list_a: list_a[0] in list_b, list_a)
                result3= [x for x in list_a if x in set2]
                print('result= ', result )
                print('result2= ', result2)
                print('result2= ', result3)

                print('Dependencia_Respuesta1    = ', Dependencia_Respuesta)

                DR= re.split('and|or', Dependencia_Respuesta)
                print('Dependencia_Respuesta2    = ', DR)
                DR_3 = [s.replace(" ", "") for s in DR]
                print('Dependencia_Respuesta3  DR_3  = ', DR_3)
                DR1= re.split('<|>|=|==|>=|<=', Dependencia_Respuesta)
                print('Dependencia_Respuesta4   = ', DR1)

                indeces = [i for i, x in enumerate(Dependencia_Respuesta) if x in a]
                print('indeces= ', indeces)
                from itertools import count
                zipped = [(i, j) for i, j in zip(count(), a) if j == Dependencia_Respuesta]
                print('zipped= ', zipped)

                StringOperator = ''.join([i for i in Dependencia_Respuesta if not i.isalpha()])
                PreguntaObj = ''.join([i for i in Dependencia_Respuesta if i.isalpha()])
                print('StringOperator   isalpha         = ', StringOperator)
                print('PreguntaObj      isalpha         = ', PreguntaObj)

                StringOperator1 = ''.join([i for i in StringOperator if not i.isdigit()])
                PreguntaObj1 = ''.join([i for i in StringOperator if i.isdigit()])
                print('StringOperator1   isdigit         = ', StringOperator1)
                print('PreguntaObj1      isdigit         = ', PreguntaObj1)
                print('StringOperator1   isdigit  list        = ', list(StringOperator1))
                print('PreguntaObj1      isdigit  list        = ', list(PreguntaObj1))
                DR = [s.replace(" ", ",") for s in StringOperator1]
                print('StringOperator1   isdigit   2      = ', DR)
                DR3 = re.split(' ', StringOperator1)
                print('PreguntaObj1      DR3      = ', DR3)
                without_empty_strings = [string for string in DR3 if string != ""]
                print('without_empty_strings      DR3      = ', without_empty_strings)

                a=['<', '>', '=', '==', '<=','>=']

                for i in range(len(a)):
                    DR_3 = [s.replace(a[i], ' ') for s in DR_3]
                print('DR_3= ', DR_3)

                DR_3 = re.split(' ', ' '.join(DR_3))
                DR_3 = [string for string in DR_3 if string != ""]

                print('*'*50)
                print('DR_3= ', DR_3)
                print('result3= ', result3)
                print('without_empty_strings = ', without_empty_strings)
                print('*'*50)
                print(list(df.columns))
                df0 = df[['Vars', 'resp', 'tipo']]
                print(df0)

                print('*'*100)
                lcolnumbertype=  df0[df0.tipo == 'number_input']['Vars'].tolist()
                print('lcolnumbertype= ', lcolnumbertype)

                print('*'*100)

                df1=df[['Vars', 'resp']]
                df1=df0[['Vars', 'resp']]

                df1 = df1.set_index('Vars')
                print('df1= ', df1)

                df2 = df1.T
                print('df2=', df2)
                for i in range(len(lcolnumbertype)):
                    print('i= ', i)
                    try:
                        #df2[lcolnumbertype[i]] = df2[lcolnumbertype[i]].fillna(0)
                        df2[lcolnumbertype[i]].astype('int')
                        #df2[lcolnumbertype[i]].astype(str).astype(float).astype(int)
                        #df2[lcolnumbertype[i]].astype('Int32')
                        #df2[lcolnumbertype[i]].astype(float).astype('Int64')
                        df2[lcolnumbertype[i]]=       pd.to_numeric(df2[lcolnumbertype[i]], errors='coerce').astype('Int64')


                        #df2[lcolnumbertype[i]].astype(np.float).astype("Int32")
                    except:
                        print('Error de conversion ')
                        print('lcolnumbertype[i]= ',lcolnumbertype[i] )
                        print('df.columns= ', df.columns)
                        print(df[['tipo', 'Vars']])
                        print('df2[lcolnumbertype[i]].values= ', df2[lcolnumbertype[i]].values[0])
                        print('_>', df[df.Vars == lcolnumbertype[i]]['tipo'].values[0])

                        print('i df2[lcolnumbertype[i]]=', df2[lcolnumbertype[i]])
                        print('f df2[lcolnumbertype[i]]=', str(df2[lcolnumbertype[i]].values[0]))
                        if str(df[df.Vars == lcolnumbertype[i]]['tipo'].values[0]) == 'number_input' :
                            print('dentro de If ')
                            df2[lcolnumbertype[i]] = df2[lcolnumbertype[i]].fillna(0)
                        else:
                            print('Fuera de If ')


                        #df2[lcolnumbertype[i]].astype(np.float).astype("Int32")
                        try:
                            df2[lcolnumbertype[i]].astype(int)
                        except:
                            print('Error de conversion 2')
                            #df2[lcolnumbertype[i]]=0
                print(df2)
                print('---------------')
                print('Dependencia_Respuesta= ', Dependencia_Respuesta)
                #print('r_car_cama_cun_distanc---->= ', df2['r_car_cama_cun_distanc'] )

                #print('r_car_18_mas_total---->= ', df2['r_car_18_mas_total'] )
                print(df2.dtypes)
                print(df2.eval(Dependencia_Respuesta).values[0])
                print('---------------')

                print('*'*50)



                #a=1
                #b=1
                #c=2
                #print('a= ',a, ' b= ', b, ' c=', c)
                #if a > 2 and b>2 or c==2:
                #    print('Dentro de If ')
                #else:
                #    print('Dentro de else ')



                #if DependenciaSiNo == '' and Dependencia_Respuesta != '' \
                #        and ops[StringOperator](df[(df['q_'] == int(Dependencia))]['resp'].values[0] ,int(PreguntaObj)):
                if df2.eval(Dependencia_Respuesta).values[0]:
                    number = st.number_input(q, step=1, min_value=0)
                    st.write('Seleccionaste: ', number)
                    df['resp'][x] = number

        if Validar != '':
            temp_string = Validar
            s = Validar

            StringOperator = ''.join([i for i in s if not i.isdigit()])
            PreguntaObj = ''.join([i for i in s if i.isdigit()])

            # st.write('temp_string: ', temp_string)
            # st.write('StringOperator: ', StringOperator)
            # st.write('PreguntaObj: ', PreguntaObj)

            # st.write('PreguntaObj PreguntaObj   : ', df[(df['q_'] == int(PreguntaObj))]['resp'].values[0])
            # st.write(' x=',x,' q=', q,' op=', op,' qe=', qe)

            # st.write('PreguntaObj x            : ', df[(df['q_'] == int(x))]['resp'].values[0])
            # st.write('Operador r3: ',StringOperator ,' ---- ',  ops[StringOperator](df[(df['q_'] == int(PreguntaObj))]['resp'].values[0], df[(df['q_'] == int(x))]['resp'].values[0]))

            if ops[StringOperator](df[(df['q_'] == int(x))]['resp'].values[0],
                                   df[(df['q_'] == int(PreguntaObj))]['resp'].values[0]):
                print('ok')
            else:
                st.error('Error de Validacion. La pregunta ' + str(x) + ' debe ser ' + str(
                    StringOperator) + ' que la pregunta ' + str(PreguntaObj))

        # print(ops["+"](1, 1))  # prints 2
        # st.write('result3: ', ops["+"](1, 1))

        # print('tipo  number_input')
        # print(df)
    if tipo == 'number_input%':
        if Dependencia == '':
            number = st.number_input(q, max_value=100)
            st.write('Seleccionaste: ', number, ' %')
            df['resp'][x] = number
        else:
            # print('--->', df[ (df['q'] == qe)]['resp'].values[0])
            if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(DependenciaSiNo):
                number = st.number_input(q, max_value=100)
                st.write('Seleccionaste: ', number, ' %')
                df['resp'][x] = number
    if tipo == 'text_input_Multiple':
        if Dependencia == '':
            st.write(q)
            a = []
            for i in range(len(op)):
                title1 = st.text_input(op[i], key=str(i))
                a.append(title1)
            st.write('Seleccionaste:', a)
            df['resp'][x] = a
        else:
            if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(DependenciaSiNo):
                # title1 = st.text_input(q , key='1')
                st.write(q)
                a = []
                for i in range(len(op)):
                    title1 = st.text_input(op[i], key=str(i))
                    a.append(title1)
                st.write('Seleccionaste:', a)
                df['resp'][x] = a
    if tipo == 'number_input_Multiple':
        if Dependencia == '':
            st.write(q)
            a = []
            for i in range(len(op)):
                # number = st.number_input(q, <<<step=1<<<)
                title1 = st.number_input(op[i], step=1, key=(str(i) + str(x)))
                a.append(title1)
            st.write('Seleccionaste:', a)
            df['resp'][x] = str(a)
        else:
            if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(DependenciaSiNo):
                # title1 = st.text_input(q , key='1')
                st.write(q)
                a = []
                for i in range(len(op)):
                    title1 = st.number_input(op[i], step=1, key=(str(i) + str(x)))
                    a.append(title1)
                st.write('Seleccionaste:', a)
                df['resp'][x] = str(a)

    if tipo == 'Ipress_Metadata':
        DFMetadata = CargaMetadata('Ipress_Metadata')
        if vista != 'No':
            print('Ipress_Metadata-================')
            print('op= ', op)

            # DFMetadata=CargaMetadata('Ipress_Metadata')
            dflocal = DFMetadata
            # print(dflocal.head())
            t = st.session_state
            print('st.session_state= ', t)
            result = [x for x in t if x.startswith('Metadata_')]
            # result= result.remove('Tipo')
            print('result1= ', result)
            # result = [x for x in result if x is not ['Metadata_Código Único',
            #                                         'Metadata_Nombre del establecimiento']]
            # l2=['Metadata_Código Único','Metadata_Nombre del establecimiento','Metadata_Departamento']
            # resultr = [x for x in result if x not in l2]
            # result.remove('Metadata_Código Único') if '' in s else None
            # result.remove('Metadata_Nombre del establecimiento') if '' in s else None
            # result.remove('Metadata_Departamento') if '' in s else None
            # result=resultr
            # print('result r= ', resultr)

            # nivel=[1,2,3,4]

            print('===========================filtros=======================================')

            print('dflocal cols', df.columns)
            print('actual   = ', op[0])
            print(df['nivel'].unique())
            dfq = df[(df['tipo'] == tipo)]
            print(dfq['nivel'].unique())

            print('Afectado = ', dfq[(dfq['nivel'].astype(int) < int(nivel))]['op'].tolist())
            lk = dfq[(dfq['nivel'].astype(int) < int(nivel))]['op'].tolist()

            for ir in lk:
                print('---->', ir)
                if op[0] != 'Departamento':
                    dflocal = dflocal[(dflocal[ir[0]] == st.session_state['Metadata_' + ir[0]])]

            print('===========================filtros=======================================')

            print('result== ', result)
            print(dflocal[['Código Único', 'Nombre del establecimiento',
                           'Departamento', 'Provincia', 'Distrito']])
            print('===========================uniques=======================================')
            print('dflocal Departamento ', dflocal['Departamento'].unique())
            print('dflocal Provincia    ', dflocal['Provincia'].unique())
            print('dflocal Distrito     ', dflocal['Distrito'].unique())

            print('===========================uniques=======================================')

            if Dependencia == '':
                optionMetadata = st.selectbox(q, dflocal[op[0]].unique().tolist(), key=('Metadata_' + op[0]))
                st.write('Seleccionaste:', optionMetadata)
                df['resp'][x] = str(optionMetadata)
                print('optionMetadata= ', optionMetadata)

                # print('optionMetadata=', st.session_state[op])
            else:
                if df[(df['q_'] == Dependencia)]['resp'].values[0] == 'Si':
                    optionMetadata = st.selectbox(q, dflocal[op[0]].unique().tolist(), key=('Metadata_' + op[0]))
                    st.write('Seleccionaste:', optionMetadata)
                    df['resp'][x] = str(optionMetadata)
                    print('optionMetadata= ', optionMetadata)
        else:
            print('colum', df.columns)
            dff = df
            print('-------------------------------------')
            dff1 = df[(df['tipo'] == tipo) & (df['Vista'] == 'No')]
            print('Max1= ', dff1['nivel'].max())
            print(dff1)
            print(dff1['op'].values[0][0])
            print('-------------------------------------')
            dff2 = df[(df['tipo'] == tipo) & (df['Vista'] != 'No')]
            print('Max2= ', dff2['nivel'].max())
            print(dff2)
            print(dff2[(dff2['nivel'] == dff1['nivel'].max())])
            tt = dff2[(dff2['nivel'] == dff1['nivel'].max())]
            print(tt)
            print('--1', tt['op'].values[0][0])
            print('--2', tt['resp'].values[0])
            print(DFMetadata[(DFMetadata[tt['op'].values[0][0]] == tt['resp'].values[0])][tt['op'].values[0][0]].values[
                      0])
            print('-------------------------------------3')

            df['resp'][x] = \
                DFMetadata[(DFMetadata[tt['op'].values[0][0]] == tt['resp'].values[0])][tt['op'].values[0][0]].values[0]
    if tipo == 'Comisarias_Metadata':
        DFMetadata = CargaMetadata('Comisarias_Metadata')
        if vista != 'No':
            print('Comisarias_Metadata-================')
            print('op= ', op)

            # DFMetadata=CargaMetadata('Ipress_Metadata')
            dflocal = DFMetadata
            # print(dflocal.head())
            t = st.session_state
            print('st.session_state= ', t)
            result = [x for x in t if x.startswith('Metadata_')]
            # result= result.remove('Tipo')
            print('result1= ', result)
            # result = [x for x in result if x is not ['Metadata_Código Único',
            #                                         'Metadata_Nombre del establecimiento']]
            # l2=['Metadata_Código Único','Metadata_Nombre del establecimiento','Metadata_Departamento']
            # resultr = [x for x in result if x not in l2]
            # result.remove('Metadata_Código Único') if '' in s else None
            # result.remove('Metadata_Nombre del establecimiento') if '' in s else None
            # result.remove('Metadata_Departamento') if '' in s else None
            # result=resultr
            # print('result r= ', resultr)

            # nivel=[1,2,3,4]

            print('===========================filtros=======================================')

            print('dflocal cols', df.columns)
            print('actual   = ', op[0])
            print(df['nivel'].unique())
            dfq = df[(df['tipo'] == tipo)]
            print(dfq['nivel'].unique())

            print('Afectado = ', dfq[(dfq['nivel'].astype(int) < int(nivel))]['op'].tolist())
            lk = dfq[(dfq['nivel'].astype(int) < int(nivel))]['op'].tolist()

            for ir in lk:
                print('---->', ir)
                if op[0] != 'Departamento':
                    dflocal = dflocal[(dflocal[ir[0]] == st.session_state['Metadata_' + ir[0]])]

            print('===========================filtros=======================================')

            print('result== ', result)
            print(dflocal[['Código Único', 'Nombre del establecimiento',
                           'Departamento', 'Provincia', 'Distrito']])
            print('===========================uniques=======================================')
            print('dflocal Departamento ', dflocal['Departamento'].unique())
            print('dflocal Provincia    ', dflocal['Provincia'].unique())
            print('dflocal Distrito     ', dflocal['Distrito'].unique())

            print('===========================uniques=======================================')

            if Dependencia == '':
                optionMetadata = st.selectbox(q, dflocal[op[0]].unique().tolist(), key=('Metadata_' + op[0]))
                st.write('Seleccionaste:', optionMetadata)
                df['resp'][x] = str(optionMetadata)
                print('optionMetadata= ', optionMetadata)

                # print('optionMetadata=', st.session_state[op])
            else:
                if df[(df['q_'] == Dependencia)]['resp'].values[0] == 'Si':
                    optionMetadata = st.selectbox(q, dflocal[op[0]].unique().tolist(), key=('Metadata_' + op[0]))
                    st.write('Seleccionaste:', optionMetadata)
                    df['resp'][x] = str(optionMetadata)
                    print('optionMetadata= ', optionMetadata)
        else:
            print('colum', df.columns)
            dff = df
            print('-------------------------------------')
            dff1 = df[(df['tipo'] == tipo) & (df['Vista'] == 'No')]
            print('Max1= ', dff1['nivel'].max())
            print(dff1)
            print(dff1['op'].values[0][0])
            print('-------------------------------------')
            dff2 = df[(df['tipo'] == tipo) & (df['Vista'] != 'No')]
            print('Max2= ', dff2['nivel'].max())
            print(dff2)
            print(dff2[(dff2['nivel'] == dff1['nivel'].max())])
            tt = dff2[(dff2['nivel'] == dff1['nivel'].max())]
            print(tt)
            print('--1', tt['op'].values[0][0])
            print('--2', tt['resp'].values[0])
            print(DFMetadata[(DFMetadata[tt['op'].values[0][0]] == tt['resp'].values[0])][tt['op'].values[0][0]].values[
                      0])
            print('-------------------------------------3')

            df['resp'][x] = \
                DFMetadata[(DFMetadata[tt['op'].values[0][0]] == tt['resp'].values[0])][tt['op'].values[0][0]].values[0]

    if tipo == 'Car_Metadata':
        DFMetadata = CargaMetadata('Car_Metadata')
        if vista != 'No':
            print('Comisarias_Metadata-================')
            print('op= ', op)

            # DFMetadata=CargaMetadata('Ipress_Metadata')
            dflocal = DFMetadata
            # print(dflocal.head())
            t = st.session_state
            print('st.session_state= ', t)
            result = [x for x in t if x.startswith('Metadata_')]
            # result= result.remove('Tipo')
            print('result1= ', result)
            # result = [x for x in result if x is not ['Metadata_Código Único',
            #                                         'Metadata_Nombre del establecimiento']]
            # l2=['Metadata_Código Único','Metadata_Nombre del establecimiento','Metadata_Departamento']
            # resultr = [x for x in result if x not in l2]
            # result.remove('Metadata_Código Único') if '' in s else None
            # result.remove('Metadata_Nombre del establecimiento') if '' in s else None
            # result.remove('Metadata_Departamento') if '' in s else None
            # result=resultr
            # print('result r= ', resultr)

            # nivel=[1,2,3,4]

            print('===========================filtros=======================================')

            print(' df', df)
            print('dflocal cols', df.columns)
            print('actual   = ', op[0])
            print(df['nivel'].unique())
            dfq = df[(df['tipo'] == tipo)]
            print(dfq['nivel'].unique())
            print('--'*50)
            print('nivel= ', nivel)

            print('dfq= ', dfq)
            print('dfq columns= ', dfq.columns)
            print('dfq[nivel]= ', dfq['nivel'])
            print('dfq[op]= ', dfq['op'])


            #print('Afectado = ', dfq[(dfq['nivel'].astype(int) < int(nivel))]['op'].tolist())
            lk = dfq[(dfq['nivel'].astype(int) < int(nivel))]['op'].tolist()

            for ir in lk:
                print('---->', ir)
                if op[0] != 'Departamento':
                    dflocal = dflocal[(dflocal[ir[0]] == st.session_state['Metadata_' + ir[0]])]

            print('===========================filtros=======================================')

            print('result== ', result)
            print(dflocal[['Código Único', 'Nombre del establecimiento',
                           'Departamento', 'Provincia', 'Distrito']])
            print('===========================uniques=======================================')
            print('dflocal Departamento ', dflocal['Departamento'].unique())
            print('dflocal Provincia    ', dflocal['Provincia'].unique())
            print('dflocal Distrito     ', dflocal['Distrito'].unique())

            print('===========================uniques=======================================')

            if Dependencia == '':
                optionMetadata = st.selectbox(q, dflocal[op[0]].unique().tolist(), key=('Metadata_' + op[0]))
                st.write('Seleccionaste:', optionMetadata)
                df['resp'][x] = str(optionMetadata)
                print('optionMetadata= ', optionMetadata)

                # print('optionMetadata=', st.session_state[op])
            else:
                if df[(df['q_'] == Dependencia)]['resp'].values[0] == 'Si':
                    optionMetadata = st.selectbox(q, dflocal[op[0]].unique().tolist(), key=('Metadata_' + op[0]))
                    st.write('Seleccionaste:', optionMetadata)
                    df['resp'][x] = str(optionMetadata)
                    print('optionMetadata= ', optionMetadata)
        else:
            print('colum', df.columns)
            dff = df
            print('-------------------------------------')
            dff1 = df[(df['tipo'] == tipo) & (df['Vista'] == 'No')]
            print('Max1= ', dff1['nivel'].max())
            print(dff1)
            print(dff1['op'].values[0][0])
            print('-------------------------------------')
            dff2 = df[(df['tipo'] == tipo) & (df['Vista'] != 'No')]
            print('Max2= ', dff2['nivel'].max())
            print(dff2)
            print(dff2[(dff2['nivel'] == dff1['nivel'].max())])
            tt = dff2[(dff2['nivel'] == dff1['nivel'].max())]
            print(tt)
            print('--1', tt['op'].values[0][0])
            print('--2', tt['resp'].values[0])
            print(DFMetadata[(DFMetadata[tt['op'].values[0][0]] == tt['resp'].values[0])][tt['op'].values[0][0]].values[
                      0])
            print('-------------------------------------3')

            df['resp'][x] = \
                DFMetadata[(DFMetadata[tt['op'].values[0][0]] == tt['resp'].values[0])][tt['op'].values[0][0]].values[0]

            print('df[resp][x] = ',df['resp'][x] )

    if tipo == 'foto':
        st.write('1folder ---> ', os.getcwd())
        st.write('1folder 2---> ', os.chdir(os.getcwd()))

        #['/app/icon-32x32.png', '/app/images/32x32.png']
        uploaded_file = '/app/icon-32x32.png'
        image = Image.open(uploaded_file)
        st.image(image, caption='Uploaded Image.', use_column_width=True)
        uploaded_file = '/app/images/32x32.png'
        image = Image.open(uploaded_file)
        st.image(image, caption='Uploaded Image.', use_column_width=True)
        print('-----------------------------------------------------1')
        print("Path at terminal when executing this file")
        print(os.getcwd() + "\n")
        print("This file path, relative to os.getcwd()")
        print(__file__ + "\n")
        print("This file full path (following symlinks)")
        full_path = os.path.realpath(__file__)
        print(full_path + "\n")
        print("This file directory and name")
        path, filename = os.path.split(full_path)
        print(path + ' --> ' + filename + "\n")
        print("This file directory only")
        print(os.path.dirname(full_path))
        print('busqueda i')
        for path, subdirs, files in os.walk(os.getcwd()):
            for name in files:
                we = os.path.join(path, name)
                # print('2=-->', we)
                name = we.split('/')[-1]
                # print('2name=-->', name)
                if (name == 'CAP.png'):
                    print('Bingo')
                #else:
                #    print('No esta causa')
        print('busqueda f')
        path = os.getcwd()

        text_files = glob.glob(path + "/**/*.png", recursive=True)

        print(text_files)
        print('busqueda f2')

        uploaded_file = st.file_uploader("Escoge las fotos a cargar: ", accept_multiple_files=True, type=['png', 'jpg'] )
        print('-----------------------------------------------------2')
        print("Path at terminal when executing this file")
        print(os.getcwd() + "\n")
        print("This file path, relative to os.getcwd()")
        print(__file__ + "\n")
        print("This file full path (following symlinks)")
        full_path = os.path.realpath(__file__)
        print(full_path + "\n")
        print("This file directory and name")
        path, filename = os.path.split(full_path)
        print(path + ' --> ' + filename + "\n")
        print("This file directory only")
        print(os.path.dirname(full_path))
        print('busqueda i')
        for path, subdirs, files in os.walk(os.getcwd()):
            for name in files:
                we = os.path.join(path, name)
                # print('2=-->', we)
                name = we.split('/')[-1]
                # print('2name=-->', name)
                if (name == 'CAP.png'):
                    print('Bingo')
                # else:
                #    print('No esta causa')
        print('busqueda f')
        path = os.getcwd()


        text_files = glob.glob(path + "/**/*.png", recursive=True)

        print(text_files)
        print('busqueda f2')


        st.write('2folder ---> ', os.getcwd())
        st.write('2folder 2---> ', os.chdir(os.getcwd()))

        if uploaded_file is not None:

            st.write('---> ', uploaded_file)
            uploaded_file='/app/CAP.png'
            image = Image.open(uploaded_file)
            st.image(image, caption='Uploaded Image.', use_column_width=True)
            st.write(".........")


        # To read file as string:
        #string_data = stringio.read()
        #st.write(string_data)
        # Can be used wherever a "file-like" object is accepted:
        #if uploaded_file is not None:
        #dataframe = pd.read_csv(uploaded_file)
        st.write('Existe folder con el nombre del formulario?')
        obj = DriveAPI()
        st.write(uploaded_file)
        folder_id = '1yhcPImMAu8AS93vj1PEByd5xYUEk94QE'
        t = obj.FolderSearch2(folder_id)
        st.write(t)
        df_t = pd.DataFrame(t)
        #global i
        st.write(df_t[(df_t['mimeType'] == 'application/vnd.google-apps.folder') & (df_t['name'] == str(page))])
        st.write(page)
        d=df_t[(df_t['mimeType'] == 'application/vnd.google-apps.folder') & (df_t['name'] == str(page))]
        if(len(d)==0):
            st.write('len   0  Crear carpeta y guardar alli ')

            f = obj.FolderCreator(str(page), folder_id)
            st.write(str(f))
            st.write('id= ', f[1])
            name2='Foto1'
            filepath=uploaded_file
            st.write('filepath= ',filepath)
            spl= str(filepath).split("'")

            st.write('filepath-spl= ',spl[1])
            st.write('uploaded_file= ', uploaded_file)

            #image = Image.open(uploaded_file)
            #st.image(image, caption='Sunrise by the mountains')

            r=obj.FileUpload_(uploaded_file, spl[1], f[1])
            st.write('r= ',r)

        else:
            st.write('len != 0 No crear y guardar en carpeta')
            st.write('id= ', d['id'].values[0])
            name2 = 'Foto1'
            filepath = uploaded_file
            st.write('filepath= ',filepath)
            spl= str(filepath).split("'")
            st.write('filepath-split= ',spl[1])
            st.write('uploaded_file= ', uploaded_file)


            #image = Image.open(uploaded_file)
            #st.image(image, caption='Sunrise by the mountains')

            r=obj.FileUpload_(uploaded_file, spl[1], d['id'].values[0])
            st.write('r= ', r)
        #st.write(uploaded_file)
    if tipo == 'number_input_Multiple_Comisarias':
        print('number_input_Multiple_Comisarias  ===========================================================')

        if Dependencia == '':
            st.write(q)
            a = []
            for i in range(len(op)):
                # number = st.number_input(q, <<<step=1<<<)
                title1 = st.number_input(op[i], step=1, key=(str(i) + str(x)))
                a.append(title1)


            st.write('Seleccionaste:', a)
            #df['resp'][x] = str(a)
            df['resp'][x] = str([",".join("{0}:{1}".format(x, y) for x, y in zip(op, a))])


            print(str(df[(df.index == int(x))]))
            df = df.append([df[(df.index == int(x))]], ignore_index=True)
            df = df.append([df[(df.index == int(x))]], ignore_index=True)

            print('len(df)= ', len(df))
            print('df.columns= ', df.columns)
            df['Vars'][(len(df) - 2)] = str(df['Vars'][x]) + ('_salida_2_s')
            df['resp'][(len(df) - 2)] = str(sum(a))




            df['Vars'][(len(df) - 1)] = str(df['Vars'][x]) + ('_salida_3_recuento')
            df['resp'][(len(df) - 1)] = str(len(a)-a.count(0))

            #test = df[['Vars','resp']].astype(str)
            #print(test)


        else:
            if df[(df['q_'] == Dependencia)]['resp'].values[0] == str(DependenciaSiNo):
                # title1 = st.text_input(q , key='1')
                st.write(q)
                a = []
                for i in range(len(op)):
                    title1 = st.number_input(op[i], step=1, key=(str(i) + str(x)))
                    a.append(title1)
                st.write('Seleccionaste:', a)
                #df['resp'][x] = str(a)
                df['resp'][x] = str([",".join("{0}:{1}".format(x, y) for x, y in zip(op, a))])

                print(str(df[(df.index == int(x))]))
                df = df.append([df[(df.index == int(x))]], ignore_index=True)
                print('len(df)= ', len(df))
                print('df.columns= ', df.columns)
                df['Vars'][(len(df) - 1)] = str(df['Vars'][x]) + ('_salida_2')
                df['resp'][(len(df) - 1)] = str(sum(a))
                test = df[['Vars', 'resp']].astype(str)
                print(test)

        print('number_input_Multiple_Comisarias  ===========================================================')


gc = pygsheets.authorize(service_file='client_secrets.json')
sh = gc.open_by_key('18-AUWmWlBRzDPv0v3KSGqeeUiWLzJ6Bp-7yoYqv6o7U')
result = str(list(sh.worksheets())).split("'")[1::2]
print('result= ', result)
result = [x for x in result if x.startswith('Formato')]
print('result Filtrado= ', result)

VentanaResultados = 'Resultados'
result.append(VentanaResultados)


@st.cache
def CargaMetadata(n):
    if (n == 'Ipress_Metadata'):
        gc = pygsheets.authorize(service_file='client_secrets.json')
        sh = gc.open_by_key('1oWQxiiaXNmvLQ2JjsG9UMBDyD7yGkXJvsHYcvK4z_fY')
        worksheet1 = sh.worksheet('title', 'BD')

        sheetData = worksheet1.get_all_records()
        print('Desde Metadata!!!')
        DFMetadata = pd.DataFrame(sheetData)
        print(DFMetadata.head())
        return DFMetadata
    if (n == 'Comisarias_Metadata'):
        gc = pygsheets.authorize(service_file='client_secrets.json')
        sh = gc.open_by_key('14-XbzALB3xZY06htbEOaoZnvC-rLWDs_AuqGoH79Y70')
        worksheet1 = sh.worksheet('title', 'BD')

        sheetData = worksheet1.get_all_records()
        print('Desde Metadata!!!')
        DFMetadata = pd.DataFrame(sheetData)
        print(DFMetadata.head())
        return DFMetadata
    if (n == 'Car_Metadata'):
        gc = pygsheets.authorize(service_file='client_secrets.json')
        sh = gc.open_by_key('1C14sOJB4qQNAVX7L2JYK7qSxI-wf2-rKzbQ70ooCRF0')
        worksheet1 = sh.worksheet('title', 'BD')

        sheetData = worksheet1.get_all_records()
        print('Desde Metadata!!!')
        DFMetadata = pd.DataFrame(sheetData)
        print(DFMetadata.head())
        return DFMetadata


# DFMetadata= CargaMetadata()
# DFMetadata=pd.DataFrame([])


page = st.sidebar.selectbox("Formularios: ", result)

# page = st.selectbox("Choose your page", result)
# x=1
print('page= ', page)

if page == VentanaResultados:
    title_ = VentanaResultados
    st.title(title_)
else:
    title_ = page[8:]
    st.title(title_)
for i in result:
    # st.write('---n= ', page)
    # x=x+1
    print('i= ', i)
    if (i == page and i != VentanaResultados):
        # option = st.selectbox('P1', ['1', '2', '3'])
        print('-----------------------------------------------------------------')
        obj = DriveAPI()
        f_id = '18-AUWmWlBRzDPv0v3KSGqeeUiWLzJ6Bp-7yoYqv6o7U'  # Repo Nuevas preg ipress
        f_name = "temp_Matriz2.xlsx"
        obj.FileDownload2(f_id, f_name)
        workbook_url = "temp_Matriz2.xlsx"
        TabFormularioActual = 'Formato_IpressTest'
        TabFormularioActual = page
        dfRepositorioE = pd.read_excel(workbook_url, sheet_name=TabFormularioActual, engine='openpyxl',
                                       keep_default_na=False)

        dfRepositorioE['Opciones2'] = ''
        for i in range(len(dfRepositorioE)):
            s = dfRepositorioE['Opciones'][i]
            result = [x.strip() for x in s.split('|')]
            dfRepositorioE['Opciones2'][i] = (result)

        # print('Columns origen = ', list(dfRepositorioE.columns))
        # print('Columns head = ', (dfRepositorioE.head))
        # print('Columns tail = ', (dfRepositorioE.tail))
        # print('Columns tail = ', (dfRepositorioE[['Opciones','Opciones2' ]]))

        # st.title("Test Form Ipress")

        dfRepositorioE['Preguntas'] = dfRepositorioE['Orden'].astype(str) + '.-' + dfRepositorioE['Preguntas']

        # ['Indicadores', 'Sec', 'Orden', 'Dependencia', 'Preguntas', 'Tipo', 'Opciones', 'Opciones2']
        df = pd.DataFrame([])
        df['sc'] = dfRepositorioE['Sec']
        df['q'] = dfRepositorioE['Preguntas']
        df['q_'] = dfRepositorioE['Orden']
        df['nivel'] = dfRepositorioE['Filtrado']
        df['op'] = dfRepositorioE['Opciones2']
        df['tipo'] = dfRepositorioE['Tipo']
        df['Dependencia'] = dfRepositorioE['Dependencia']
        df['Vars'] = dfRepositorioE['Vars']
        df['Vista'] = dfRepositorioE['Vista']
        df['DependenciaSiNo'] = dfRepositorioE['DependenciaSiNo']
        df['Validar'] = dfRepositorioE['Validar']
        df['Dependencia_Respuesta'] = dfRepositorioE['Dependencia_Respuesta']
        df['resp'] = np.nan

        duplicateRowsDF = df[df.Vars.duplicated()]
        print('=' * 100)
        print('duplicateRowsDF= ', duplicateRowsDF)
        print('len ', len(duplicateRowsDF))
        print('=' * 100)

        # df = df.head(20)
        # print(df.head(20))
        # df[(df['flag'] == 0)]
        if (len(duplicateRowsDF) > 0):
            st.write('Variables Duplicadas')
        else:

            x = 0
            for j in ((df['sc'].unique())):
                with st.expander(j):
                    for i in range(len(df[(df['sc'] == j)])):
                        dft = df[(df['sc'] == j)]
                        dft = dft.reset_index()
                        # print('i= ', i)
                        # print('---> ', df[(df['sc'] == j)]['q'])
                        # expanderrr(q[i], op[i])
                        expanderrr(x, dft['q'][i], dft['op'][i], dft['tipo'][i], dft['Dependencia'][i], dft['nivel'][i],
                                   dft['Vista'][i], dft['DependenciaSiNo'][i], dft['Validar'][i], dft['Dependencia_Respuesta'][i])
                        x = x + 1
            # print(df.head(100))
            f = st.button('Terminar')

            if f:
                with st.spinner('Esperando respuesta del servidor...'):
                    time.sleep(5)
                print('-----------------------------------------i')
                df[['resp']] = df[['resp']].fillna('')

                try:
                    # st.write('Why hello there')
                    gc = pygsheets.authorize(service_file='client_secrets.json')
                    print('-----------------------------------------f1')
                    print('Inicio del proceso de guardado ')
                    print('verificar pestaña ')
                    # TabFormularioActual

                    # gc = pygsheets.authorize(service_file='client_secrets.json')
                    sh = gc.open_by_key('1Qyw9PDK6aIBF2PozPA_uQmfJI6FtQZrrAOpf5ujxdlg')
                    result = str(list(sh.worksheets())).split("'")[1::2]
                    print('result= ', result)
                    result = [x for x in result if x.startswith('Formato')]
                    print('result Filtrado= ', result)

                    if any(TabFormularioActual in word for word in result):
                        print('Si existe la pestaña i')
                        #####################################################################################
                        print('*' * 150)
                        print('detalles carga anterior  i')
                        worksheet1 = sh.worksheet('title', TabFormularioActual)
                        sheetDataCheck = worksheet1.get_all_records()
                        print('Desde sheetDataCheck')
                        DFCheck = pd.DataFrame(sheetDataCheck)
                        # print(DFCheck.head())
                        print('LIstado de variables guardadas: ', list(DFCheck.columns))
                        x = len(list(DFCheck.columns))
                        HeaderExcelCargado = [
                            string.ascii_uppercase[i] if i < 26 else string.ascii_uppercase[i // 26 - 1] +
                                                                     string.ascii_uppercase[i % 26] for i
                            in range(x)]
                        print('HeaderExcelCargado: ', HeaderExcelCargado)

                        print('detalles carga anterior  f')

                        print('*' * 150)
                        #####################################################################################
                        print('=' * 180)
                        print('detalles carga actual i')
                        # print(df['resp'].tolist())
                        tr = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                        d = df['resp'].tolist()
                        d.append(tr)
                        print('Rsultados actuales', d)

                        x = len(d)
                        HeaderExcelNuevo = [
                            string.ascii_uppercase[i] if i < 26 else string.ascii_uppercase[i // 26 - 1] +
                                                                     string.ascii_uppercase[
                                                                         i % 26] for i in range(x)]
                        print('qw= ', HeaderExcelNuevo)

                        print('detalles carga actual f')
                        print('=' * 180)
                        #####################################################################################

                        HeaderDiferencia = list(set(HeaderExcelNuevo) - set(HeaderExcelCargado))
                        print('HeaderDiferencia= ', HeaderDiferencia)
                        print('-------------a')
                        Dfa=pd.DataFrame(HeaderExcelNuevo, columns=['a'])
                        Dfa['i']=Dfa.index
                        print(Dfa.head())
                        print(len(Dfa))
                        print('-------------b')
                        Dfb = pd.DataFrame(HeaderDiferencia, columns=['a'])
                        print(Dfb.head())
                        print(len(Dfb))
                        Dfb['b']=0
                        for i in range(len(HeaderDiferencia)):
                            print('HeaderDiferencia[i]= ', HeaderDiferencia[i])
                            print(' 1=', Dfb[Dfb.a == HeaderDiferencia[i]])
                            print(' 2=',Dfa[Dfa.a == HeaderDiferencia[i]])
                            print(' 3=',Dfa[Dfa.a == HeaderDiferencia[i]]['i'])

                            #Dfb.loc[Dfa[0] == (int(UltimoTimestamp) + 0.0), 'BS'] =
                            Dfb['b'][i] = Dfa[Dfa.a == HeaderDiferencia[i]]['i']
                        print(Dfb.head())
                        Dfb=Dfb.sort_values(by=['b'])
                        print(Dfb.head())
                        print('-------------b')
                        HeaderDiferencia=sorted(HeaderDiferencia)
                        HeaderDiferencia = Dfb['a'].tolist()
                        print('HeaderDiferencia= ', HeaderDiferencia)
                        # res = [1 if ele in search_list else 0 for ele in test_list]
                        mask = np.in1d(HeaderExcelNuevo, HeaderDiferencia)
                        # print('mask= ', mask)
                        # print('list mask= ', list(mask))

                        print('n list dmask =', len(mask))
                        print('n list d     =', len(d))
                        dvars = DFCheck.columns.tolist()
                        print('list dvars= ', (dvars))

                        print('n list dvars     =', len(dvars))

                        dd = np.array(d)
                        print('d mask values = ', dd[(mask)])
                        # print('dvars.tolist()= ', dvars.tolist())
                        # dvars = np.array(dvars)
                        # print('dvars mask values = ', dvars[(mask)])

                        print(list(df.columns))
                        dvars = df['Vars'].tolist()
                        print('dvars = ', dvars)
                        print('n list dvars =', len(dvars))
                        dvars = dvars.append('Fecha')

                        Diff = list(set(df['Vars'].tolist()) - set(DFCheck.columns.tolist()))
                        print('Diff= ', Diff)
                        print(df[['resp', 'Vars']].tail())

                        # print( df[(df['Vars'] == ['var95', 'var94'])])
                        #print(df.Vars.isin(['var95', 'var94']))
                        #print(df[df.Vars.isin(['var95', 'var94'])]['resp'].tolist())
                        List1 = HeaderDiferencia
                        string = "1"
                        output = ["{}{}".format(i, string) for i in List1]

                        print('output   = ', output)
                        print('Diff     = ', Diff)

                        print('len(output)= ', len(output))
                        x = []
                        if len(output) > 1:
                            print('>1')
                            x = (str(output[0]) + ':' + str(output[-1]))
                        if len(output) == 0:
                            print('==0')
                            x = []
                        if len(output) == 1:
                            print('=1')
                            x = str(output[0])
                        print('x=', x)
                        if len(output) > 0:
                            # for i in range(len(output)):
                            #    print(output[i],' - ',Diff[i] )
                            #    worksheet1.update_value(str(output[i]), str(Diff[i]))
                            # print((str(output[0])+':'+str(output[-1])))
                            print('a')
                            print('x= ', x )
                            print('[Diff]= ', [Diff])

                            worksheet1.update_values(x, [Diff], extend=True)
                            print('b')

                        print('Obtener listado luego de update ')
                        worksheet1 = sh.worksheet('title', TabFormularioActual)
                        sheetDataCheck = worksheet1.get_all_records()
                        DFCheck = pd.DataFrame(sheetDataCheck)
                        print('LIstado de variables guardadas: ', list(DFCheck.columns))
                        print('Listado para nsertar ', df['Vars'].tolist())

                        dfinsert = df[['Vars', 'resp']]
                        print('a')
                        tr = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                        print('b')
                        dfinsert = dfinsert.append({'Vars': 'Fecha', 'resp': tr}, ignore_index=True)
                        print('dfinsert1= ', dfinsert)
                        dfinsert = dfinsert.set_index('Vars')
                        print('dfinsert1.1= ', dfinsert)

                        dfinsert = dfinsert.transpose()
                        print('dfinsert1.2= ', dfinsert)
                        print('list(DFCheck.columns)=', list(dfinsert.columns))

                        print('Diferencia ____________________')
                        Difer = list(set(list(DFCheck.columns)) - set(list(dfinsert.columns)))
                        print('Difer1= ', Difer)

                        print('DFCheck= ', DFCheck)
                        print('dfinsert= ', dfinsert)

                        for i in range(len(Difer)):
                            print(i, '   -  ', Difer[i])
                            # DFCheck = DFCheck.append({'Vars': Difer[i]}, ignore_index=True)
                            # dfinsert = dfinsert.append({Difer[i]:'' }, ignore_index=True)
                            dfinsert[Difer[i]] = ''

                        print('Diferencia ____________________')
                        print(list(DFCheck.columns))

                        dfinsert = dfinsert[list(DFCheck.columns)]
                        print('dfinsert2= ', dfinsert)
                        # d= dfinsert.tolist()
                        dfinsert = dfinsert.transpose()
                        d = dfinsert.resp.tolist()
                        print('d=', d)
                        print('Si existe la pestaña f')

                        if d[0] == '':
                            print('Vacio')
                            st.warning('Requiere llenado de id de Monitor')
                        else:
                            print('lleno')
                            worksheet1.append_table(values=d)
                            sheetData = worksheet1.get_all_records
                            st.success('Hecho!')
                            st.balloons()

                        #####################################################################################

                    else:
                        print('No existe la pestaña ')

                        sh.add_worksheet(TabFormularioActual)  # Please set the new sheet name.
                        print('1')
                        listcabecera = df['Vars'].tolist()
                        lenlistcabecera = len(listcabecera)
                        listcabecera.append('Fecha')
                        print('2')
                        worksheet1 = sh.worksheet('title', TabFormularioActual)
                        print('3')
                        worksheet1.append_table(values=listcabecera)
                        print('4')

                        worksheet1 = sh.worksheet('title', TabFormularioActual)  # choose worksheet to work with
                        # print(df['resp'].tolist())
                        tr = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                        d = df['resp'].tolist()
                        d.append(tr)
                        print(d)

                        if d[0] == '':
                            print('Vacio')
                            st.warning('Requiere llenado de id de Monitor')
                        else:
                            print('lleno')
                            worksheet1.append_table(values=d)
                            sheetData = worksheet1.get_all_records
                            st.success('Hecho!')
                            st.balloons()

                except ValueError as e:

                    print('Error= ', e)

                    st.error('Error!... volver a intentar')

    if i == page and i == VentanaResultados:
        # st.title(VentanaResultados)
        st.write('Desde resultados ')

        gc = pygsheets.authorize(service_file='client_secrets.json')
        sh = gc.open_by_key('1Qyw9PDK6aIBF2PozPA_uQmfJI6FtQZrrAOpf5ujxdlg')
        result = str(list(sh.worksheets())).split("'")[1::2]
        print('result= ', result)
        result = [x for x in result if x.startswith('Formato')]
        print('result Filtrado= ', result)
        #st.write('result=  ', result)

        option = st.selectbox('Pestaña a procesar: ', result)
        worksheet1 = sh.worksheet('title', option)
        sheetDataCheck = worksheet1.get_all_records()
        sheetDataCheck = pd.DataFrame(sheetDataCheck)
        # #################################################################
        st.write('-' * 80)
        st.write('Resultados Raw Data')
        print('Resultados Raw Data')
        DfRaw = sheetDataCheck.tail()
        st.dataframe(data=DfRaw, width=None, height=None)
        # #################################################################
        st.write('-' * 80)
        st.write('Resultados Mod')
        print('Resultados Mod')

        DfMod = sheetDataCheck

        def addSem(DfInd, strFecha):
            workbook_url = 'Semana.xlsx'
            TabFormularioActual = 'Semana'
            dfsemana = pd.read_excel(workbook_url, sheet_name=TabFormularioActual, engine='openpyxl',
                                     keep_default_na=False)
            dftsemana = date_expander(dfsemana, 'I_Sem', 'F_Sem', 'd', 'r', True)

            dftsemana['r'] = dftsemana['r'].astype("string")
            dftsemana['r'] = pd.Series(dftsemana['r'], dtype="string")
            dftsemana['r'] = dftsemana['r'].str[:10]
            dftsemana['r2'] = dftsemana['r'].str.replace(r'\D', '')
            dftsemana['r2'] = dftsemana.r2.apply(int)

            dft1 = DfInd
            dft1[strFecha] = dft1[strFecha].astype("string")
            dft1[strFecha] = pd.Series(dft1[strFecha], dtype="string")
            #dft1['Fecha_Full'] = dft1['Fecha']
            dft1[strFecha] = dft1[strFecha].str[:10]
            dft1[strFecha] = dft1[strFecha].str.replace(r'\D', '')
            dft1[strFecha] = dft1.Fecha.apply(int)

            dft2 = pd.merge(dftsemana, dft1, left_on='r2', right_on='Fecha', how='right')
            return dft2


        DfMod=addSem(DfMod,'Fecha')
        listCol = list(DfMod.columns)
        #st.write('l istCol= ', listCol)
        #listCol = listCol.remove("Fecha")
        listCol=[s for s in listCol if s != 'Fecha']
        #st.write('listCol= ', listCol)
        #DfMod['D1'] = DfMod[listCol].duplicated()
        print(DfMod)
        DfMod['Duplicado'] = DfMod[listCol].duplicated(keep='last')
        print(DfMod)
        DfMod1 = DfMod.astype(str)
        st.dataframe(data=DfMod1, width=None, height=None)
        DfMod = DfMod[DfMod.Duplicado == False]
        st.write('Sin duplicados ')
        DfMod1 = DfMod.astype(str)
        st.dataframe(data=DfMod1, width=None, height=None)

        # #################################################################
        st.write('-'*80)
        st.write('Resultados Indicadores')
        print('Resultados Mod', '#'*100)
        print(DfMod)
        DfInd = sheetDataCheck.tail()
        DfInd = DfMod
        print('ok 0')
        gc = pygsheets.authorize(service_file='client_secrets.json')
        sh = gc.open_by_key('18-AUWmWlBRzDPv0v3KSGqeeUiWLzJ6Bp-7yoYqv6o7U')
        worksheet1 = sh.worksheet('title', 'Indicadores')
        sheetDataCheck = worksheet1.get_all_records()
        sheetDataCheck = pd.DataFrame(sheetDataCheck)
        print('ok 1')
        print('sheetDataCheck= ', sheetDataCheck)
        #st.write('sheetDataCheck.columns= ', sheetDataCheck.columns)

        # ------

        #workbook_url = 'Semana.xlsx'
        #TabFormularioActual = 'Semana'
        #dfsemana = pd.read_excel(workbook_url, sheet_name=TabFormularioActual, engine='openpyxl',
        #                               keep_default_na=False)

        #dftsemana = date_expander(dfsemana, 'I_Sem', 'F_Sem', 'd', 'r', True)
        #print(dftsemana)

        #dftsemana['r'] = dftsemana['r'].astype("string")
        #dftsemana['r'] = pd.Series(dftsemana['r'], dtype="string")
        #dftsemana['r'] = dftsemana['r'].str[:10]
        #dftsemana['r2'] = dftsemana['r'].str.replace(r'\D', '')
        #dftsemana['r2'] = dftsemana.r2.apply(int)

        #dft1 = DfInd
        #dft1['Fecha'] = dft1['Fecha'].astype("string")
        #dft1['Fecha'] = pd.Series(dft1['Fecha'], dtype="string")
        #dft1['Fecha_Full'] = dft1['Fecha']
        #dft1['Fecha'] = dft1['Fecha'].str[:10]
        #dft1['Fecha'] = dft1['Fecha'].str.replace(r'\D', '')
        #dft1['Fecha'] = dft1.Fecha.apply(int)



        #dft2 = pd.merge(dftsemana, dft1, left_on='r2', right_on='Fecha', how='right')

        #print(dft2[['semana_f', 'Fecha']])
        #DfInd=dft2
        # ------


        #sheetDataCheck=dft2
        dff1 = sheetDataCheck[(sheetDataCheck['Sector'] == str(option))]
        dff1 = dff1.reset_index()

        #st.write('dff1= ', dff1)

        #st.write('Nombre_Indicador 1= ', dff1['Nombre_Indicador'][0])
        #st.write('Nombre_Indicador 2= ', dff1['Nombre_Indicador'][1])
        # st.dataframe(data=dff1, width=None, height=None)

        # formula = "var5/var6"
        # ind='Indi1'
        # DfInd[ind]=DfInd.eval(formula)

        # st.write('Nombre_Indicador  = ',dff1['Nombre_Indicador'].values)
        # st.write('Formula           = ', dff1['Formula'].values)
        print('dff1= ', dff1)
        ListDfIndCols = list(DfInd.columns)
        print('ListDfIndCols= ', ListDfIndCols)

        ListInd = []
        #try:
        for i in range(len(dff1)):
            print(i, '#'*20)
            #DfInd[dff1['Nombre_Indicador'].values[0]] = DfInd.eval(dff1['Formula'].values[0])
            #print(DfInd.dtypes)
            #pd.to_numeric(DfInd, errors='ignore')
            #DfInd=DfInd.apply(pd.to_numeric, errors='ignore')
            #DfInd=DfInd.convert_dtypes()
            #DfInd=DfInd.infer_objects()
            #DfInd=DfInd.convert_dtypes()
            # ##########################################################

            
            #print('DfInd= ', DfInd)
            #df0 = df[['Vars', 'resp', 'tipo']]
            #print(df0)
            #print('*' * 100)
            #lcolnumbertype = df0[df0.tipo == 'number_input']['Vars'].tolist()
            #print('lcolnumbertype= ', lcolnumbertype)
            #print('*' * 100)
            #df1 = df[['Vars', 'resp']]
            #df1 = df0[['Vars', 'resp']]
            #df1 = df1.set_index('Vars')
            #print(df1)
            #df2 = df1.T
            #print(df2)
            #for i in range(len(lcolnumbertype)):
            #    print('i= ', i)
            #    try:
            #        df2[lcolnumbertype[i]].astype('int')
            #        df2[lcolnumbertype[i]] = pd.to_numeric(df2[lcolnumbertype[i]], errors='coerce').astype('Int64')
            #    except:
            #        print('Error de conversion ')
            #        df2[lcolnumbertype[i]] = df2[lcolnumbertype[i]].fillna(0)
            #        df2[lcolnumbertype[i]].astype(int)
            #print(df2)
            
            #print('*' * 50)

            # ################################################################

            #(var67 / var44)
            # DfInd = DfInd.astype({"var5": float, "var6": float})
            #print(DfInd[['var67','var44']].dtypes)
            #DfInd = DfInd.astype({"var67": float, "var44": float})
            #print(DfInd[['var67', 'var44']].dtypes)


            #lsc=DfInd.columns
            #for i in range(len(lsc)):
            #    print(i,' lsc[i]= ',   lsc[i])
            #    DfInd = DfInd.astype({lsc[i]: int})

            #print(DfInd.dtypes)
            #print('dff1[Nombre_Indicador][i]]=   ', dff1['Nombre_Indicador'][i] )
            #print('dff1[Formula][i]=             ', dff1['Formula'][i])
            #print('DfInd.eval(dff1[Formula][i])= ', DfInd.eval(dff1['Formula'][i]) )
            #print(DfInd)
            #DfInd[dff1['Nombre_Indicador'][i]] = DfInd.eval(dff1['Formula'][i])
            #ListInd.append(dff1['Nombre_Indicador'][i])
            #print(DfInd)
        #except:
        #    print('Error')

        print('-'*100) # ##########################################################
        #data = {'a': [1, 2, 3]}
        #df = pd.DataFrame({'a': [1, 2, 3]})
        #df.if(df.a > 2, 42, 0 )
        #print('df= ', df)
        #df.eval('where(a>2, 42, 0)')
        #print('df= ', df)
        #print(DfInd)
        #DfInd2=DfInd.eval("op = var7 * 1 if var7 < 2 else var7", inplace=True)
        # 'where(a>2, 42, 0)
        #DfInd2 = DfInd.eval("where(var7 < 2,var7 * 1,var7 * 3)", inplace=True)
        #print(DfInd2)


        with_s = [x for x in list(dff1.columns) if x.startswith('Sub')]

        dft = dff1[with_s]
        for j in range(len(dft)):
            print('j= ',j,'='*200)
            print('j= ', j)
            for i in range(len(with_s)):
                print('i= ', i,'-' * 200)
                print('dft[with_s[i]][j]= ', dft[with_s[i]][j])
                if dft[with_s[i]][j] != '':
                    print('dff1= ', dff1)
                    print('dff1 columns = ', dff1.columns)

                    print('with_s= ', with_s)
                    #with_s= with_s +['Ind']
                    print('with_s= ', with_s)
                    print('dft= ', dft)
                    #print('dft= ', dft['Sub1'])
                    import re
                    print('i= ', i, ' j= ',j)
                    print('with_s[i]]= ', with_s[i])
                    print('dft[with_s[i]]=' ,  dft[with_s[i]])
                    print('dft[with_s[i]][j]=' ,  dft[with_s[i]][j])

                    print('dft[with_s[i]].values[0]= ', dft[with_s[i]].values[0])

                    #a1 = re.split("'('|')'", dft[with_s[i]].values[0])
                    a1 = re.split("'('|')'", dft[with_s[i]][j])

                    print('a1= ', a1)
                    #a2=re.findall('\[[^\]]*\]|\([^\)]*\)|\"[^\"]*\"|\S+', dft[with_s[i]].values[0])
                    a2=re.findall('\[[^\]]*\]|\([^\)]*\)|\"[^\"]*\"|\S+', dft[with_s[i]][j])

                    print('a2= ', a2)
                    a3 = re.split(",|;", a2[1])
                    a3 = [s.replace("(", "") for s in a3]
                    print('a3= ', a3)
                    print('str(a3[0])= ', str(a3[0]))
                    print('****************************')
                    print('DfInd= ', DfInd)
                    a33=str(a3[0])
                    print('a33= ', a33)
                    #a33 = re.split("isnull|isnull()", a33)
                    #a33 = [s.replace("isnull", "isnull()") for s in a33]
                    a33=a33.replace("isnull", "isnull()")
                    a33=a33.replace("isna", "isna()")

                    print('a33= ', a33)
                    print('a33= ', str(a33))

                    #Result = DfInd.eval((a33))
                    #print('Result= ', Result)

                    try:
                        Result = DfInd.eval((a33))
                        print('Result= ', Result)
                        print('****************************')

                        print('a3[1]= ', a3[1])
                        Result = Result.replace({True: int(a3[1]), False: int(a3[2])})
                        print('Result= ', Result)

                        DfInd[with_s[i]] = Result
                    except:
                        print('Error')

                    print(dff1)
                    print(dff1['Ind'])

                    print('dff1[Ind][j]= ', dff1['Ind'][j] )
                    a22 = re.findall('\[[^\]]*\]|\([^\)]*\)|\"[^\"]*\"|\S+', dff1['Ind'][j])
                    print('a22= ', a22)
                    a3 = re.split(",|;", a22[1])
                    a3 = [s.replace("(", "") for s in a3]
                    print('a3= ', a3)
                    try:
                        Result = DfInd.eval(str(a3[0]))
                        Result = Result.replace({True: int(a3[1]), False: int(a3[2])})
                        print('Result= ', Result)
                        print('dff1[Nombre_Indicador][j]= ', dff1['Nombre_Indicador'][j])
                        # print('dff1[Nombre_Indicador][j]= ', dff1['Nombre_Indicador'][j])
                        DfInd[dff1['Nombre_Indicador'][j]] = Result
                        print('1 DfInd= ', DfInd)
                        aa = str(with_s[i])
                        bb = str((with_s[i] + '_' + str(j)))
                        print('aa= ', aa, ' bb= ', bb)
                        # DfInd=DfInd.rename(columns={aa: bb}, inplace=True)
                        # df2_tidy = df2_melted.rename(columns={'variable': 'Year', 'value': 'Income'}, inplace=False)
                        DfInd[bb] = DfInd[aa]
                        DfInd.drop(aa, axis='columns', inplace=True)
                        print('2 DfInd= ', DfInd)
                    except:
                        print('error')



                else:
                    print('ingreso a else ')

                    try:
                        a22 = re.findall('\[[^\]]*\]|\([^\)]*\)|\"[^\"]*\"|\S+', dff1['Ind'][j])
                        print('a22= ', a22)
                        a3 = re.split(",|;", a22[1])
                        a3 = [s.replace("(", "") for s in a3]
                        print('a3= ', a3)
                        print('str(a3[0])= ', str(a3[0]))
                        Result = DfInd.eval(str(a3[0]))
                        Result = Result.replace({True: int(a3[1]), False: int(a3[2])})
                        print('Result= ', Result)
                        DfInd[dff1['Nombre_Indicador'][j]] = Result
                        print('1 DfInd= ', DfInd)
                    except:
                        print('error')

        print('-'*100) # ##########################################################


        cols = DfInd.columns.tolist()
        cols = [cols[-1]] + cols[:-1]  # or whatever change you need

        DfInd = DfInd.reindex(columns=cols)
        DfIndCopy= DfInd
        DfInd = DfInd.astype(str)
        st.dataframe(data=DfInd, width=None, height=None)

        #picture = st.camera_input("Take a picture")

        #if picture:
        #    st.image(picture)


        st.write('- ' * 80)
        Menu = st.selectbox('Menu', ['Modo Exploracion', 'Modo Construccion'])

        if Menu == 'Modo Exploracion':


            ColTiempo=['colsem', 'colsem2', 'semana_f', 'I_Mes', 'F_Mes', 'I_Semana_Cr', 'F_Semana_Cr', 'I_year', 'F_year']
            print('cols= ', cols)
            print('ColTiempo= ', ColTiempo)
            ColDiff= set(cols) - set(ColTiempo)
            print('ColDiff= ', (ColDiff))


            # Conversion de columnas
            ColAgrupacion = []
            ColAgregacion = []
            DfIndP = DfInd
            ColDiff=list(ColDiff)
            for i in range(len((ColDiff))):
                try:
                    DfIndP[ColDiff[i]] = DfIndP[ColDiff[i]].astype(int)
                    ColAgregacion.append(ColDiff[i])
                except:
                    ColAgrupacion.append(ColDiff[i])
            print('*'*100)
            print('ColAgrupacion= ', ColAgrupacion)
            print('ColAgregacion= ', ColAgregacion)
            print('*' * 100)


            st.write('-' * 80)
            st.write('Eje Temporal')
            ColtiempoSelect = st.multiselect('Columnas de tiempo', ColTiempo)
            st.write('- ' * 80)
            st.write('Agrupación ')
            ColAgrupacionSelect = st.multiselect('Columnas para Agrupar ', (ColAgrupacion))
            st.write('Agregación ')
            ColAgregacionSelect = st.multiselect('Columnas para Agregar ', (ColAgregacion))
            st.write('- ' * 80)
            ColTotalSelect= ColtiempoSelect + ColAgrupacionSelect + ColAgregacionSelect
            print('ColTotalSelect= ', ColTotalSelect)
            st.write('Datos Seleccionados: ')

            DfIndStr = DfInd[ColTotalSelect].astype(str)
            st.dataframe(data=DfIndStr, width=None, height=None)

            print('dff1= ', dff1)
            print('dff1.columns= ', dff1.columns)

            #str_list = re.split(",|;| ", dff1['operacion_de_agregacion'][0])
            #print('str_list= ', str_list)

            #str_list = list(filter(None, str_list))
            #print('str_list= ', str_list)
            ColTotalSelect = ColtiempoSelect + ColAgrupacionSelect + ColAgregacionSelect

            b = []
            for i in range(len(ColAgregacionSelect)):
                b.append(['sum', 'min', 'max', 'mean'])
            # b= [['sum', 'max', 'mean']*len(ColTotalSelect)]
            print('b= ', b)
            res = dict(zip(ColAgregacionSelect, b))
            print('res= ', res)

            print(DfIndP)
            if len(ColAgregacionSelect) > 0 and len(ColtiempoSelect) > 0 and len(ColAgrupacionSelect)>0 :
                DfIndAg = DfIndP.groupby(ColtiempoSelect+ ColAgrupacionSelect).agg(res)
                #DfIndAg = DfIndP.groupby('I_Mes').agg({
                #    '%(3)': ['sum', 'max'],
                #    '%(4)': 'mean',
                #    'Sub3_4': ['sum', 'max', 'mean'],
                #    'Sub1_2': ['sum', 'max', 'mean']
                #})
                print(DfIndAg)
                DfIndAg.columns = ['_'.join(col) for col in DfIndAg.columns.values]
                print(DfIndAg)

                st.write('Datos Seleccionados - Agregados: ')
                DfIndStr = DfIndAg.astype(str)
                st.dataframe(data=DfIndStr, width=None, height=None)
            st.write('- ' * 80)

        #st.write('- ' * 80)
        if Menu == 'Modo Construccion':

            col01c1, col02c1 = st.columns(2)
            #print('DfIndAg= ', DfIndAg)

            with col01c1:
                QueAgregar = st.radio(
                    "Columnas a incluir",
                    ('Agregar Todo',
                     'Agregar Todo solo con tipo de Agregacion',
                     'Agregar solo Indicadores con tipo de Agregacion',
                     'Agregar solo Indicadores y Subindicadores con tipo de Agregacion'))
            with col02c1:
                TipoAgregacion = st.selectbox('Tipo de Agregacion', ['sum', 'min', 'max', 'mean'])
            SeleccionManual = st.checkbox('Seleccion Manual ')

            def generarDf():
                ColTiempo = ['colsem', 'colsem2', 'semana_f', 'I_Mes', 'F_Mes', 'I_Semana_Cr', 'F_Semana_Cr', 'I_year',
                             'F_year']
                print('cols= ', cols)
                print('ColTiempo= ', ColTiempo)
                ColDiff = set(cols) - set(ColTiempo)
                print('ColDiff= ', (ColDiff))

                # Conversion de columnas
                ColAgrupacion = []
                ColAgregacion = []
                DfIndP = DfInd
                ColDiff = list(ColDiff)
                for i in range(len((ColDiff))):
                    try:
                        DfIndP[ColDiff[i]] = DfIndP[ColDiff[i]].astype(int)
                        ColAgregacion.append(ColDiff[i])
                    except:
                        ColAgrupacion.append(ColDiff[i])
                print('*' * 100)
                print('ColAgrupacion= ', ColAgrupacion)
                print('ColAgregacion= ', ColAgregacion)
                print('*' * 100)

                #st.write('-' * 80)
                #st.write('Eje Temporal')
                #ColtiempoSelect = st.multiselect('Columnas de tiempo', ColTiempo)
                #st.write('- ' * 80)
                #st.write('Agrupación ')
                #ColAgrupacionSelect = st.multiselect('Columnas para Agrupar ', (ColAgrupacion))
                #st.write('Agregación ')
                #ColAgregacionSelect = st.multiselect('Columnas para Agregar ', (ColAgregacion))
                #st.write('- ' * 80)
                #ColTotalSelect = ColtiempoSelect + ColAgrupacionSelect + ColAgregacionSelect
                #print('ColTotalSelect= ', ColTotalSelect)
                ColTotal= ColTiempo + ColAgrupacion + ColAgregacion
                print('ColTotal= ', ColTotal)

                #st.write('Datos Seleccionados: ')

                #DfIndStr = DfInd[ColTotal].astype(str)
                #st.dataframe(data=DfIndStr, width=None, height=None)

                print('dff1= ', dff1)
                print('dff1.columns= ', dff1.columns)

                #str_list = re.split(",|;| ", dff1['operacion_de_agregacion'][0])
                #print('str_list= ', str_list)

                #str_list = list(filter(None, str_list))
                #print('str_list= ', str_list)
                #ColTotalSelect = ColtiempoSelect + ColAgrupacionSelect + ColAgregacionSelect

                b = []
                for i in range(len(ColAgregacion)):
                    b.append(['sum', 'min', 'max', 'mean'])
                # b= [['sum', 'max', 'mean']*len(ColTotalSelect)]
                #print('b= ', b)
                res = dict(zip(ColAgregacion, b))
                #print('res= ', res)

                #print(DfIndP)
                if len(ColAgregacion) > 0 and len(ColTiempo) > 0 and len(ColAgrupacion) > 0:
                    DfIndAg = DfIndP.groupby(ColTiempo + ColAgrupacion).agg(res)
                    # DfIndAg = DfIndP.groupby('I_Mes').agg({
                    #    '%(3)': ['sum', 'max'],
                    #    '%(4)': 'mean',
                    #    'Sub3_4': ['sum', 'max', 'mean'],
                    #    'Sub1_2': ['sum', 'max', 'mean']
                    # })
                    print(DfIndAg)
                    print(DfIndAg.columns)
                    #DfIndAg['inde']=DfIndAg.index
                    DfIndAg = DfIndAg.reset_index()
                    print(DfIndAg)

                    DfIndAg.columns = ['_'.join(col) for col in DfIndAg.columns.values]
                    print(DfIndAg)
                    print(DfIndAg.columns)


                return DfIndAg

            DfIndAg= generarDf()
            print(DfIndAg)
            print()
            print()
            print()
            print()
            print()
            print()
            print()
            print()

            TodaslasColumnas= list(DfIndAg.columns)
            ListNameIndi = list(dff1['Nombre_Indicador'])
            ColTiempo = ['colsem', 'colsem2', 'semana_f', 'I_Mes', 'F_Mes', 'I_Semana_Cr', 'F_Semana_Cr', 'I_year',
                         'F_year']
            Agregacion= ['sum', 'min', 'max', 'mean']
            print('TodaslasColumnas= ', TodaslasColumnas)
            pref_list = ["Sub"]
            ColumnasSub = [ele for ele in TodaslasColumnas if any(ele.startswith(el) for el in pref_list)]
            print('ColumnasSub= ', ColumnasSub)
            ColumnasSubAgregacion = [ele for ele in ColumnasSub if any(ele.endswith(el) for el in [TipoAgregacion])]
            print('ColumnasSubAgregacion= ', ColumnasSubAgregacion)
            ColumnasInd = [ele for ele in TodaslasColumnas if any(ele.startswith(el) for el in ListNameIndi)]
            print('ColumnasInd= ', ColumnasInd)
            ColumnasIndAgregacion = [ele for ele in ColumnasInd if any(ele.endswith(el) for el in [TipoAgregacion])]
            print('ColumnasSubAgregacion= ', ColumnasSubAgregacion)
            ColumnasTiempo = [ele for ele in TodaslasColumnas if any(ele.startswith(el) for el in ColTiempo)]
            print('ColumnasTiempo= ', ColumnasTiempo)
            TodoconTipoAgregacion= [ele for ele in TodaslasColumnas if any(ele.endswith(el) for el in [TipoAgregacion])]
            print('TodoconTipoAgregacion= ', TodoconTipoAgregacion)
            TodoconAgregacion= [ele for ele in TodaslasColumnas if any(ele.endswith(el) for el in Agregacion)]
            print('TodoconAgregacion= ', TodoconAgregacion)
            TodoAgrupacion = set(TodaslasColumnas) - set(TodoconAgregacion) - set(ColumnasTiempo)
            TodoAgrupacion = list(TodoAgrupacion)
            print('TodoAgrupacion= ', TodoAgrupacion)

            ColTiempoPrecargado=[]
            ColAgrupacionPrecargado=[]
            ColAgregacionPrecargado=[]

            if QueAgregar == 'Agregar Todo':
                print('1')
                ColTiempoPrecargado=ColumnasTiempo
                ColAgrupacionPrecargado=TodoAgrupacion
                ColAgregacionPrecargado=TodoconAgregacion
            if QueAgregar == 'Agregar Todo solo con tipo de Agregacion':
                print('2')
                ColTiempoPrecargado=ColumnasTiempo
                ColAgrupacionPrecargado=TodoAgrupacion
                ColAgregacionPrecargado= ColumnasIndAgregacion
            if QueAgregar == 'Agregar solo Indicadores con tipo de Agregacion':
                print('3')
                ColTiempoPrecargado = ColumnasTiempo
                ColAgrupacionPrecargado = TodoAgrupacion
                ColAgregacionPrecargado = ColumnasIndAgregacion
            if QueAgregar == 'Agregar solo Indicadores y Subindicadores con tipo de Agregacion':
                print('4')
                ColTiempoPrecargado = ColumnasTiempo
                ColAgrupacionPrecargado = TodoAgrupacion
                ColAgregacionPrecargado = ColumnasIndAgregacion + ColumnasSubAgregacion



            if SeleccionManual:
                st.write('-' * 80)
                st.write('Eje Temporal')
                ColtiempoSelect = st.multiselect('Columnas de tiempo', ColumnasTiempo, ColTiempoPrecargado)
                st.write('- ' * 80)
                st.write('Agrupación ')
                ColAgrupacionSelect = st.multiselect('Columnas para Agrupar ', (TodoAgrupacion), ColAgrupacionPrecargado)
                st.write('Agregación ')
                ColAgregacionSelect = st.multiselect('Columnas para Agregar ', (TodoconAgregacion),ColAgregacionPrecargado )
                st.write('- ' * 80)
            else:
                ColtiempoSelect= ColTiempoPrecargado
                ColAgrupacionSelect=ColAgrupacionPrecargado
                ColAgregacionSelect=ColAgregacionPrecargado

            print()
            print()
            print('ColtiempoSelect=     ', ColtiempoSelect)
            print('ColAgrupacionSelect= ', ColAgrupacionSelect)
            print('ColAgregacionSelect= ', ColAgregacionSelect)

            ColtiempoSelectRecalculo=[]
            ColAgrupacionSelectRecalculo=[]
            ColAgregacionSelectRecalculo_a=[]
            ColAgregacionSelectRecalculo_b=[]

            for i in range(len(ColtiempoSelect)):
                ColtiempoSelectRecalculo.append(ColtiempoSelect[i][:-1])

            for i in range(len(ColAgrupacionSelect)):
                ColAgrupacionSelectRecalculo.append(ColAgrupacionSelect[i][:-1])
            for i in range(len(ColAgregacionSelect)):
                str_list = re.split("_sum|_min|_mean|_max", ColAgregacionSelect[i])
                str_list = [string for string in str_list if string != ""]
                ColAgregacionSelectRecalculo_a.append(str_list[0])
                ft = (ColAgregacionSelect[i])
                str_list = ft.split(str_list[0])
                str_list = [string for string in str_list if string != ""]
                #str_list = re.split("_", str_list)
                ColAgregacionSelectRecalculo_b.append(str_list[0][1:])

            print()
            print('ColtiempoSelectRecalculo=            ', ColtiempoSelectRecalculo)
            print('ColAgrupacionSelectRecalculo=        ', ColAgrupacionSelectRecalculo)
            print('ColAgregacionSelectRecalculo_a=      ', ColAgregacionSelectRecalculo_a)
            print('ColAgregacionSelectRecalculo_b=      ', ColAgregacionSelectRecalculo_b)


            aa= ColtiempoSelectRecalculo + ColAgrupacionSelectRecalculo
            lst1 = ColAgregacionSelectRecalculo_a
            lst2 = ColAgregacionSelectRecalculo_b
            lst_tuple = list(zip(lst1, lst2))
            print('lst_tuple= ', lst_tuple)
            from collections import defaultdict

            d = defaultdict(list)
            for k, v in lst_tuple:
                d[k].append(v)

            print('(d.items())= ', (d.items()))

            print('sorted(d.items())= ', sorted(d.items()))
            print('dict(d.items())= ', dict(d.items()))


            bb = dict(zip(ColAgregacionSelectRecalculo_a, ColAgregacionSelectRecalculo_b))
            bb= dict(d.items())
            #bb = {ColAgregacionSelectRecalculo_a[i]: ColAgregacionSelectRecalculo_b[i] for i in range(len(ColAgregacionSelectRecalculo_a))}
            print('aa= ', aa)
            print('bb= ', bb)
            print('DfIndCopy.columns= ', DfIndCopy.columns)

            print('DfIndCopy= ', DfIndCopy)

            #print('DfIndCopy= ', DfIndCopy[bb].head())

            for i in range (len(ColAgregacionSelectRecalculo_a)):
                #DfIndCopy[ColAgregacionSelectRecalculo_a[i]].astype('float')
                #DfIndCopy[ColAgregacionSelectRecalculo_a[i]].astype(str).astype('float').astype(int)
                DfIndCopy[ColAgregacionSelectRecalculo_a[i]] = pd.to_numeric(DfIndCopy[ColAgregacionSelectRecalculo_a[i]])


            #print((DfIndCopy[bb].dtypes))

            print('-'*100, 'DfIndAg', '-'*90)
            DfIndAg = DfIndCopy.groupby(aa).agg(bb)
            print(DfIndAg)
            print('-'*200)

            #DfIndAg = DfIndAg.reset_index()
            #DfIndAg.columns = ['_'.join(col) for col in DfIndAg.columns.values]



            print('-'*100, 'reset index  test df', '-'*60)
            DfIndAg = DfIndAg.reset_index()

            DfIndAg.columns = ['_'.join(col).strip() for col in DfIndAg.columns.values]
            #DfIndAg.columns = DfIndAg.columns.get_level_values(0)
            print(DfIndAg)
            print('-' * 200)

            #st.dataframe(data=DfIndAg, width=None, height=None)
            print('-'*100, 'reset index  DfIndAg', '-'*80)

            DfIndAg = DfIndAg.reset_index(drop=False)
            print(DfIndAg)
            print('-'*200)

            #st.dataframe(data=DfIndAg, width=None, height=None)

            print('-'*200)
            print('DfIndAg list = ', list(DfIndAg.columns))
            print('-'*200)
            print('ColAgregacionSelectRecalculo_a=  ', ColAgregacionSelectRecalculo_a)
            print('-'*200)
            print('ColAgregacionSelect=             ', ColAgregacionSelect)
            print('-'*200)

            """"""
            #for i in range(len(ColAgregacionSelectRecalculo_a)):
            #    DfIndAg[ColAgregacionSelect[i]] = DfIndAg[ColAgregacionSelectRecalculo_a[i]]
            #    DfIndAg.drop(ColAgregacionSelectRecalculo_a[i], axis='columns', inplace=True)
            #a= ColtiempoSelect + ColAgrupacionSelect
            #b= ColtiempoSelectRecalculo +  ColAgrupacionSelectRecalculo
            #for i in range(len(a)):
            #    DfIndAg[a[i]] = DfIndAg[b[i]]
            #    DfIndAg.drop(b[i], axis='columns', inplace=True)



            #DfIndAg.columns = ['_'.join(col) for col in DfIndAg.columns.values]
            print(DfIndAg)
            st.write('Matriz Resultado: ')

            st.dataframe(data=DfIndAg, width=None, height=None)

            #from st_aggrid import AgGrid
            #AgGrid(DfIndAg)
            #DfInd = DfIndAg[ColtiempoSelect+ColAgrupacionSelect+ColAgregacionSelect].astype(str)
            #st.dataframe(data=DfInd, width=None, height=None)

            st.write('Matriz Resultado - Melt: ')


            a=ColtiempoSelect+ ColAgrupacionSelect
            #a= ColAgrupacionSelect
            b= ColAgregacionSelect
            print('a= ',a)
            print('b= ',b)

            DfInd = DfIndAg.melt(id_vars=a, value_vars=b, value_name='Valor',var_name='Descripcion')
            print(DfInd)
            #DfInd = DfInd.astype(str)
            st.dataframe(data=DfInd, width=None, height=None)

            st.write('- ' * 80)



        # #################################################################
        #st.write('-'*80)

        col1, col2, col3 = st.columns(3)

        tr = datetime.strptime(dt.datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d')
        with col1:
            f_nameg = 'MejorGasto_RawData_' + str(tr)[:10] + '.xlsx'
            sheet_name = 'MejorGasto_'
            writer = pd.ExcelWriter(f_nameg, engine='xlsxwriter')
            DfRaw.to_excel(writer, sheet_name=sheet_name, index=False)
            writer.save()

            with open(f_nameg, 'rb') as f:
                st.download_button('Download Raw Data', f, file_name=f_nameg)  # Defaults to 'application/octet-stream'

        with col2:
            f_nameg = 'MejorGasto_Mod_' + str(tr)[:10] + '.xlsx'
            sheet_name = 'MejorGasto_'
            writer = pd.ExcelWriter(f_nameg, engine='xlsxwriter')
            DfMod.to_excel(writer, sheet_name=sheet_name, index=False)
            writer.save()

            with open(f_nameg, 'rb') as f:
                st.download_button('Download Modificado', f,
                                   file_name=f_nameg)  # Defaults to 'application/octet-stream'

        with col3:
            f_nameg = 'MejorGasto_Indicadores_' + str(tr)[:10] + '.xlsx'
            sheet_name = 'Indicadores'
            writer = pd.ExcelWriter(f_nameg, engine='xlsxwriter')
            DfInd.to_excel(writer, sheet_name=sheet_name, index=False)
            writer.save()

            with open(f_nameg, 'rb') as f:
                st.download_button('Download Indicadores', f,
                                   file_name=f_nameg)  # Defaults to 'application/octet-stream'


### 10 if var67 == 1 else 0

# data.groupby('Group').sum().eval('one_two = One / Two')

# df.eval("op = op * @mult if index < @ex_date else op", inplace=True)