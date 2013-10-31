#!/usr/bin/python

import argparse, base64, getpass, json, mimetypes, oauth2, platform, requests, urllib2
from requests import Request

class AuthenticationFailure(Exception):
  """The provided email address and password were incorrect."""
  def __init__(self, what):
    self.what = "Authentication failure: " + what

class AuthenticationWarning(Exception):
  """For non-fatal errors during the authentication process."""
  def __init__(self, what):
    self.what = "Authentication warning: " + what


class U1Driver:
  BASE_API_PATH = "https://one.ubuntu.com/api/file_storage/v1"
  BASE_FILES_PATH = "https://files.one.ubuntu.com"
  CREDS_FILE = '.credentials'
  
  def __init__(self, resource_host="https://edge.one.ubuntu.com", content_host="https://files.one.ubuntu.com"):
    self.email = ''
    self.passwd = ''
    self.session = requests.Session()
    self.resource_host = resource_host
    self.content_host = content_host

  def oauth_signed_request(self, req):
    """Signs any request using OAuth. Mandatory before any request of access upon OAuth-protected data"""
    oauth_request = oauth2.Request.from_consumer_and_token(self.consumer, self.token, req.method, req.url)
    oauth_request.sign_request(oauth2.SignatureMethod_PLAINTEXT(), self.consumer, self.token)
    for header,value in oauth_request.to_header().items():
      req.headers[header] = value
    rep = self.session.send(req.prepare())
    rep.raise_for_status()
    return rep

  def oauth_make_credentials(self, jsoncreds):
    """Creates the OAuth-related classes for credentials (used for every signed request)"""
    self.consumer = oauth2.Consumer(jsoncreds['consumer_key'], jsoncreds['consumer_secret'])
    self.token = oauth2.Token(jsoncreds['token'], jsoncreds['token_secret'])

  def oauth_get_access_token(self):
    """Acquire an OAuth access token for the given user credentials."""
    #get hostname
    description = 'Ubuntu One @ {}'.format(platform.node())
    # Issue a new access token for the user.
    params = {'ws.op': 'authenticate',
              'token_name': description}
    headers = {'Accept': 'application/json',
               'Authorization': 'Basic {}'.format(base64.b64encode('{}:{}'.format(self.email, self.passwd)))
               }
    rep = requests.get('https://login.ubuntu.com/api/1.0/authentications', params=params, headers=headers)
    rep.raise_for_status()
    credentials = rep.json()
    return credentials

  def authenticate(self, credentials):
    """Makes first signed request using new OAuth credentials"""
    self.oauth_make_credentials(credentials)
    # Tell Ubuntu One about the new OAuth token.
    req = Request('GET', 'https://one.ubuntu.com/oauth/sso-finished-so-get-tokens/')
    rep = self.oauth_signed_request(req)
    

  def prompt_user_credentials(self):
    """Asks the user for his U1 credentials."""
    self.email = raw_input("Your Ubuntu One email: ")
    self.passwd = getpass.getpass("The Ubuntu One password for " + self.email + ": ")

  def credentials_from_email_and_password(self):
    self.prompt_user_credentials()
    return self.oauth_get_access_token()

  def credentials_from_file(self, credsfile):
    """Extracts the OAuth credentials from file. If it fails, asks user if he wants to input his credentials.
    Attempts to store it in given file name."""
    try:
      with open(credsfile) as f:
        jsoncreds = json.loads(f.read())
    except IOError as e: # open failed
      response = raw_input("Credentials file is unreadable or doesn't exist.\nWould you like to input your U1 credentials instead (will attempt storing the credentials in " + U1Driver.CREDS_FILE + ") ? [Yn] ")
      if response == '' or response.upper() == 'Y':
        jsoncreds = self.credentials_from_email_and_password()
      else:
        raise AuthenticationFailure("Cannot open credentials file (" + credsfile + ")")
    except ValueError: # file opened, but unable to json.loads the contents
      response = raw_input("Credentials file contents corrupted.\nWould you like to input your U1 credentials instead (will attempt storing the credentials in " + U1Driver.CREDS_FILE + ") ? [Yn] ")
      if response == '' or response.upper() == 'Y':
        jsoncreds = self.credentials_from_email_and_password()
      else:
        raise AuthenticationFailure("Cannot open credentials file (" + credsfile + ")")
    return jsoncreds

  def login(self, prompt, creds_file=CREDS_FILE):
    """Log in method using OAuth signed requests"""
    if prompt:
      credentials = self.credentials_from_email_and_password()
    else:
      credentials = self.credentials_from_file(creds_file)
    try:
      rep = self.authenticate(credentials)
    except requests.exceptions.HTTPError as httpe: # credentials revoked
      print httpe
      rep = raw_input("Your credentials may have been revoked. Regenerate it using your email/password ? [Yn] ")
      if rep == '' or rep.upper() == 'Y':
        self.prompt_user_credentials()
        credentials = self.oauth_get_access_token()
      else: # unhandled error
        raise
    jsondata = json.dumps(credentials)
    try:
      with open(creds_file, 'wb') as f:
        f.write(jsondata)
    except IOError:
      raise AuthenticationWarning("Could not save credentials to " + creds_file + " file")

  def logout(self):
    pass
  
  def get(self, args, bytes_range=''):
    """Downloads a remote file. It is a two-step process:
    first acquire the metadatas on the API, then download the actual content of the file from the Files address."""
    url = U1Driver.BASE_API_PATH 
    url += '/~/' if not args[0].startswith('/~/') else ''
    url += urllib2.quote(args[0])
    req = Request('GET', url)
    print 'Sending request to', url
    try:
      rep = self.oauth_signed_request(req)
    except requests.exceptions.HTTPError as httpe:
        print httpe, "upon access to", url
        return
    metadatas = rep.json()
    url = U1Driver.BASE_FILES_PATH + urllib2.quote(metadatas.get('content_path'), safe="/")
    req = Request('GET', url)
    if bytes_range != '':
      req.headers['Range'] = 'bytes=' + bytes_range
    print 'Downloading contents of', url
    try:
      rep = self.oauth_signed_request(req)
    except requests.exceptions.HTTPError as httpe:
      print httpe, "while downloading", url
      return
    content = rep.content
    try:
      local_filename = args[1]
    except IndexError:
      local_filename = args[0].split('/')[-1]
    print 'Writing contents to', local_filename
    try:
      with open(local_filename, 'wb') as local_file:
        local_file.write(content)
    except IOError:
      print "Unable to write to file", local_filename
    print 'Done'

  def put(self, args, bytes_range=''):
    """Uploads a file on the Ubuntu One cloud.
    A two-step process: first notify the API about the creation of the file, then upload the actual content of the file to the Files domain"""
    url = U1Driver.BASE_API_PATH
    url += '/~/' if not args[0].startswith('/~/') else ''
    url += urllib2.quote(args[1])
    req = Request('PUT', url, data='{"kind":"file"}')
    try:
      rep = self.oauth_signed_request(req)
    except requests.exceptions.HTTPError as httpe:
      print "Error creating", url, ":", httpe
      return
    jsondata = rep.json()
    print rep
    print jsondata
    try:
      with open(args[0], 'rb') as local_file:
        bytedata = bytearray(local_file.read())  
    except IOError:
      print 'Remote file was created, but local file', args[0], 'cannot be read'
      return
    url = U1Driver.BASE_FILES_PATH + urllib2.quote(jsondata.get('content_path'), safe="/~")
    req = Request('PUT', url,
                  headers={'Content-Length': str(len(bytedata)),
                           'Content-Type': mimetypes.guess_type(args[0])[0] or 'application/octet-stream', # if mimetypes failed to guess
                           'Range': 'bytes=0-1',
                           },
                  data=bytedata
                  )
    try:
      rep = self.oauth_signed_request(req)
    except requests.exceptions.HTTPError as httpe:
      print "Error uploading to", url, ":", httpe
      return
    print 'Complete'

  def delete(self, file_path, *args):
    pass

  def list_files(self, volume='', tabs_nbr=0):
    """Lists files on the Ubuntu One cloud.
    Will print the contents of each Ubuntu One volume of the user through a recursive call"""
    url = U1Driver.BASE_API_PATH + urllib2.quote(volume)
    request = requests.Request('GET', url, params={'include_children': 'true'})
    try:
      rep = self.oauth_signed_request(request)
    except requests.exceptions.HTTPError:
        print "Error finding:", url
        return
    basepaths = rep.json()
    if volume == '': # base call: list every subsequent directory (recursively)
      self.list_files(basepaths['root_node_path'])
      for user_volume in basepaths['user_node_paths']:
        self.list_files(user_volume)
    else: # lists volume and each subdirectory recursively
      print ('  '*tabs_nbr) + basepaths.get('resource_path').split('/')[-1] + '/'
      if basepaths.get('has_children') == True:
        for child in basepaths.get('children'):
          if child['kind'] == 'directory':
            self.list_files(child['resource_path'], tabs_nbr+1)
          elif child['kind'] == 'file':
            print ('  '*(tabs_nbr+1)) + child[u'path'].split('/')[-1]

  def quit(self):
    pass

  def interactive(self):
    cmds = { "quit": self.quit,
             "list": self.list_files,
             "get": self.get,
             "put": self.put,
             "delete": self.delete
      }
    while True:
      command = raw_input("U1Driver>>> ")
      words = command.split()
      try:
        if command != '':
          cmds[words[0]](words[1:]) if words[0] != 'list' else self.list_files()
      except KeyError:
        print "Unknown command:", words[0]
      except IndexError:
        print "This command expects arguments (at least a file/volume name)."

def main():
  driver = U1Driver()

  parser = argparse.ArgumentParser(description="Test Onitu Driver for Ubuntu One", epilog="2013 Onitu")
  parser.add_argument('-p', action='store_true', default=False, dest='prompt', help='Prompts user identification')
  args = parser.parse_args()
  try:
    driver.login(args.prompt)
  except AuthenticationFailure as af:
    print af.what
  except AuthenticationWarning as aw:
    print "Successfully logged in, but errors occurred (", aw.what, ")"
  else:
    print "Successfully logged in"
    driver.interactive()
        
if __name__ == "__main__":
  main()
