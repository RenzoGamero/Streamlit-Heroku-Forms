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
        # Extract the file name out of the file path
        name = filepath.split('/')[-1]

        # Find the MimeType of the file
        mimetype = MimeTypes().guess_type(name)[0]
        # print('mimetype= ', mimetype )
        # create file metadata
        file_metadata = {'name': name2, 'parents': [folder_id]}

        try:
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

            return file.get('webViewLink')


        except:
            print('Error FileUpload_ ')
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
    if tipo == 'foto':
        if qe == '':
            option = st.selectbox(q, op)
            st.write('Seleccionaste:', option)
            df['resp'][x] = option
        else:
            if df[(df['q_'] == qe)]['resp'].values[0] == str(DependenciaSiNo):
                option = st.selectbox(q, op)
                st.write('Seleccionaste:', option)
                df['resp'][x] = option


def expanderrr(x, q, op, tipo, qe, nivel, vista, DependenciaSiNo, Validar):
    # print('=============================================================')
    # print('X= ', x)
    # print('q= ', q)
    # print('qe= ', qe)
    if tipo == 'selectbox':
        if qe == '':
            option = st.selectbox(q, op)
            st.write('Seleccionaste:', option)
            df['resp'][x] = option
        else:
            if df[(df['q_'] == qe)]['resp'].values[0] == str(DependenciaSiNo):
                option = st.selectbox(q, op)
                st.write('Seleccionaste:', option)
                df['resp'][x] = option
    if tipo == 'multiselect':
        if qe == '':
            options = st.multiselect(q, op)
            st.write('Seleccionaste:', options)
            df['resp'][x] = str(options)
        else:
            if df[(df['q_'] == qe)]['resp'].values[0] == str(DependenciaSiNo):
                options = st.multiselect(q, op)
                st.write('Seleccionaste:', options)
                df['resp'][x] = str(options)
    if tipo == 'text_input':
        if qe == '':
            titles = st.text_input(q, key='text_input1' + str(page))
            st.write('Seleccionaste:', titles)
            df['resp'][x] = titles
        else:
            if df[(df['q_'] == qe)]['resp'].values[0] == str(DependenciaSiNo):
                titles = st.text_input(q, key='text_input2' + str(page))
                st.write('Seleccionaste:', titles)
                df['resp'][x] = titles
    if tipo == 'radio':
        if qe == '':
            genre = st.radio(q, (op))
            st.write('Seleccionaste:', genre)
            df['resp'][x] = genre
        else:
            # print('--->', df[(df['q'] == qe)]['resp'].values[0])
            if df[(df['q_'] == qe)]['resp'].values[0] == str(DependenciaSiNo):
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
            if df[(df['q_'] == qe)]['resp'].values[0] == str(DependenciaSiNo):
                Fecha = st.date_input(q,
                                      min_value=dt.datetime.today() + dt.timedelta(days=-30),
                                      max_value=dt.datetime.today() + dt.timedelta(days=14))
                st.write('Fecha:', Fecha)
                df['resp'][x] = str(Fecha.strftime('%Y-%m-%d'))
    if tipo == 'number_input':

        if qe == '':
            # print('qe vacio ')
            number = st.number_input(q, step=1, min_value=0)
            st.write('Seleccionaste: ', number)
            df['resp'][x] = number
        else:

            if df[(df['q_'] == qe)]['resp'].values[0] == str(DependenciaSiNo):
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
        if qe == '':
            number = st.number_input(q, max_value=100)
            st.write('Seleccionaste: ', number, ' %')
            df['resp'][x] = number
        else:
            # print('--->', df[ (df['q'] == qe)]['resp'].values[0])
            if df[(df['q_'] == qe)]['resp'].values[0] == str(DependenciaSiNo):
                number = st.number_input(q, max_value=100)
                st.write('Seleccionaste: ', number, ' %')
                df['resp'][x] = number

    if tipo == 'text_input_Multiple':
        if qe == '':
            st.write(q)
            a = []
            for i in range(len(op)):
                title1 = st.text_input(op[i], key=str(i))
                a.append(title1)
            st.write('Seleccionaste:', a)
            df['resp'][x] = a
        else:
            if df[(df['q_'] == qe)]['resp'].values[0] == str(DependenciaSiNo):
                # title1 = st.text_input(q , key='1')
                st.write(q)
                a = []
                for i in range(len(op)):
                    title1 = st.text_input(op[i], key=str(i))
                    a.append(title1)
                st.write('Seleccionaste:', a)
                df['resp'][x] = a
    if tipo == 'number_input_Multiple':
        if qe == '':
            st.write(q)
            a = []
            for i in range(len(op)):
                # number = st.number_input(q, <<<step=1<<<)
                title1 = st.number_input(op[i], step=1, key=(str(i) + str(x)))
                a.append(title1)
            st.write('Seleccionaste:', a)
            df['resp'][x] = str(a)
        else:
            if df[(df['q_'] == qe)]['resp'].values[0] == str(DependenciaSiNo):
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

            if qe == '':
                optionMetadata = st.selectbox(q, dflocal[op[0]].unique().tolist(), key=('Metadata_' + op[0]))
                st.write('Seleccionaste:', optionMetadata)
                df['resp'][x] = str(optionMetadata)
                print('optionMetadata= ', optionMetadata)

                # print('optionMetadata=', st.session_state[op])
            else:
                if df[(df['q_'] == qe)]['resp'].values[0] == 'Si':
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

            if qe == '':
                optionMetadata = st.selectbox(q, dflocal[op[0]].unique().tolist(), key=('Metadata_' + op[0]))
                st.write('Seleccionaste:', optionMetadata)
                df['resp'][x] = str(optionMetadata)
                print('optionMetadata= ', optionMetadata)

                # print('optionMetadata=', st.session_state[op])
            else:
                if df[(df['q_'] == qe)]['resp'].values[0] == 'Si':
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

    if tipo == 'foto':
        uploaded_file = st.file_uploader("Escoge las fotos a cargar: ", accept_multiple_files=True, type=['png', 'jpg'] )
        # To read file as string:
        #string_data = stringio.read()
        #st.write(string_data)



        # Can be used wherever a "file-like" object is accepted:
        #if uploaded_file is not None:
        #dataframe = pd.read_csv(uploaded_file)
        st.write(uploaded_file)

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
        df['qe'] = dfRepositorioE['Dependencia']
        df['Vars'] = dfRepositorioE['Vars']
        df['Vista'] = dfRepositorioE['Vista']
        df['DependenciaSiNo'] = dfRepositorioE['DependenciaSiNo']
        df['Validar'] = dfRepositorioE['Validar']

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
                        expanderrr(x, dft['q'][i], dft['op'][i], dft['tipo'][i], dft['qe'][i], dft['nivel'][i],
                                   dft['Vista'][i], dft['DependenciaSiNo'][i], dft['Validar'][i])
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
                        print(df.Vars.isin(['var95', 'var94']))
                        print(df[df.Vars.isin(['var95', 'var94'])]['resp'].tolist())
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


                except Exception as e:

                    print(e)

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
        DfRaw = sheetDataCheck.tail()
        st.dataframe(data=DfRaw, width=None, height=None)
        # #################################################################
        st.write('-' * 80)
        st.write('Resultados Mod')
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
        DfMod['Duplicado'] = DfMod[listCol].duplicated(keep='last')




        st.dataframe(data=DfMod, width=None, height=None)
        DfMod = DfMod[DfMod.Duplicado == False]
        st.write('Sin duplicados ')
        st.dataframe(data=DfMod, width=None, height=None)

        # #################################################################
        st.write('-'*80)

        st.write('Resultados Indicadores')
        DfInd = sheetDataCheck.tail()
        DfInd = DfMod

        gc = pygsheets.authorize(service_file='client_secrets.json')
        sh = gc.open_by_key('18-AUWmWlBRzDPv0v3KSGqeeUiWLzJ6Bp-7yoYqv6o7U')
        worksheet1 = sh.worksheet('title', 'Indicadores')
        sheetDataCheck = worksheet1.get_all_records()
        sheetDataCheck = pd.DataFrame(sheetDataCheck)

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
        #st.write('dff1= ', dff1)

        #st.write('Nombre_Indicador 1= ', dff1['Nombre_Indicador'][0])
        #st.write('Nombre_Indicador 2= ', dff1['Nombre_Indicador'][1])
        # st.dataframe(data=dff1, width=None, height=None)

        # formula = "var5/var6"
        # ind='Indi1'
        # DfInd[ind]=DfInd.eval(formula)

        # st.write('Nombre_Indicador  = ',dff1['Nombre_Indicador'].values)
        # st.write('Formula           = ', dff1['Formula'].values)
        ListDfIndCols = list(DfInd.columns)
        ListInd = []
        try:
            for i in range(len(dff1)):
                #DfInd[dff1['Nombre_Indicador'].values[0]] = DfInd.eval(dff1['Formula'].values[0])
                DfInd[dff1['Nombre_Indicador'][i]] = DfInd.eval(dff1['Formula'][i])
                ListInd.append(dff1['Nombre_Indicador'][i])
        except:
            print('Error')

        cols = DfInd.columns.tolist()
        cols = [cols[-1]] + cols[:-1]  # or whatever change you need
        DfInd = DfInd.reindex(columns=cols)

        st.dataframe(data=DfInd, width=None, height=None)

        st.write('Melt ')
        DfInd['ID']=DfInd.index

        DfInd = DfInd.melt(id_vars=ListDfIndCols, value_vars=ListInd, var_name='Indicador')
        st.dataframe(data=DfInd, width=None, height=None)

        # #################################################################
        st.write('-'*80)

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
