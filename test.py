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
            titles = st.text_input(q, key='text_input1'+ str(page))
            st.write('Seleccionaste:', titles)
            df['resp'][x] = titles
        else:
            if df[(df['q_'] == qe)]['resp'].values[0] == 'Si':
                titles = st.text_input(q, key='text_input2'+ str(page))
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
def expanderrr(x, q, op, tipo, qe):
    #print('=============================================================')
    #print('X= ', x)
    #print('q= ', q)
    #print('qe= ', qe)
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
            titles = st.text_input(q, key='text_input1'+ str(page))
            st.write('Seleccionaste:', titles)
            df['resp'][x] = titles
        else:
            if df[(df['q_'] == qe)]['resp'].values[0] == 'Si':
                titles = st.text_input(q, key='text_input2'+ str(page))
                st.write('Seleccionaste:', titles)
                df['resp'][x] = titles
    if tipo == 'radio':
        if qe == '':
            genre = st.radio(q, (op))
            st.write('Seleccionaste:', genre)
            df['resp'][x] = genre
        else:
            #print('--->', df[(df['q'] == qe)]['resp'].values[0])
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
        #print('tipo  number_input')
        #print(df)
        if qe == '':
            #print('qe vacio ')
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

gc = pygsheets.authorize(service_file='client_secrets.json')
sh = gc.open_by_key('18-AUWmWlBRzDPv0v3KSGqeeUiWLzJ6Bp-7yoYqv6o7U')
result = str(list(sh.worksheets())).split("'")[1::2]
print('result= ', result)
result = [x for x in result if x.startswith('Formato')]
print('result Filtrado= ', result)



page = st.sidebar.selectbox("SideBar de prueba ", result)

#page = st.selectbox("Choose your page", result)
#x=1
st.title(page[8:])
for i in page:
    #st.write('---n= ', page)
    #x=x+1

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

    #print('Columns origen = ', list(dfRepositorioE.columns))
    # print('Columns head = ', (dfRepositorioE.head))
    # print('Columns tail = ', (dfRepositorioE.tail))
    # print('Columns tail = ', (dfRepositorioE[['Opciones','Opciones2' ]]))

    #st.title("Test Form Ipress")



    dfRepositorioE['Preguntas'] = dfRepositorioE['Orden'].astype(str) + '.-' + dfRepositorioE['Preguntas']

    # ['Indicadores', 'Sec', 'Orden', 'Dependencia', 'Preguntas', 'Tipo', 'Opciones', 'Opciones2']
    df = pd.DataFrame([])
    df['sc'] = dfRepositorioE['Sec']
    df['q'] = dfRepositorioE['Preguntas']
    df['q_'] = dfRepositorioE['Orden']

    df['op'] = dfRepositorioE['Opciones2']
    df['tipo'] = dfRepositorioE['Tipo']
    df['qe'] = dfRepositorioE['Dependencia']
    df['Vars'] = dfRepositorioE['Vars']
    df['resp'] = np.nan
    # df = df.head(20)
    #print(df.head(20))






    # df[(df['flag'] == 0)]
    x = 0
    for j in ((df['sc'].unique())):
        with st.expander(j):
            for i in range(len(df[(df['sc'] == j)])):
                dft = df[(df['sc'] == j)]
                dft = dft.reset_index()
                # print('i= ', i)
                # print('---> ', df[(df['sc'] == j)]['q'])
                # expanderrr(q[i], op[i])
                expanderrr(x, dft['q'][i], dft['op'][i], dft['tipo'][i], dft['qe'][i])
                x = x + 1

    #print(df.head(100))
    f = st.button('Terminar')

    if f:
        with st.spinner('Esperando respuesta del servidor...'):
            time.sleep(5)
        print('-----------------------------------------i')

        try:
            # st.write('Why hello there')
            gc = pygsheets.authorize(service_file='client_secrets.json')
            print('-----------------------------------------f1')
            print('Inicio del proceso de guardado ')
            print('verificar pesta??a ')
            # TabFormularioActual

            # gc = pygsheets.authorize(service_file='client_secrets.json')
            sh = gc.open_by_key('1Qyw9PDK6aIBF2PozPA_uQmfJI6FtQZrrAOpf5ujxdlg')
            result = str(list(sh.worksheets())).split("'")[1::2]
            print('result= ', result)
            result = [x for x in result if x.startswith('Formato')]
            print('result Filtrado= ', result)

            if any(TabFormularioActual in word for word in result):
                print('Si existe la pesta??a ')
            else:
                print('No existe la pesta??a ')

                sh.add_worksheet(TabFormularioActual)  # Please set the new sheet name.
                print('1')
                listcabecera = df['Vars'].tolist()
                lenlistcabecera = len(listcabecera)
                listcabecera.append('var' + str(lenlistcabecera))
                print('2')
                worksheet1 = sh.worksheet('title', TabFormularioActual)
                print('3')
                worksheet1.append_table(values=listcabecera)
                print('4')

            # sh = gc.open('Repositorio-Streamlit-Heroku')  # Open GoogleSheet
            # sh = gc.open_by_key('1Qyw9PDK6aIBF2PozPA_uQmfJI6FtQZrrAOpf5ujxdlg')
            worksheet1 = sh.worksheet('title', TabFormularioActual)  # choose worksheet to work with
            print('-----------------------------------------f2')

            print(df['resp'].tolist())
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

        except:

            st.error('Error!... volver a intentar')
