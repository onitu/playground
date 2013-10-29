#!/usr/bin/python

import argparse, base64, getpass, json, oauth2, platform, urllib, urllib2

class AuthenticationFailure(Exception):
  """The provided email address and password were incorrect."""
  def __init__(self, what):
    self.what = "Authentication failure: " + what

class AuthenticationWarning(Exception):
  """For non-fatal errors during the authentication process."""
  def __init__(self, what):
    self.what = "Authentication warning: " + what


class U1Driver:
  BASE_API_PATH = "/api/file_storage/v1"
  CREDS_FILE = '.credentials'
  
  def __init__(self, resource_host="https://edge.one.ubuntu.com", content_host="https://files.one.ubuntu.com"):
    self.email = ''
    self.passwd = ''
    self.resource_host = resource_host
    self.content_host = content_host

  def oauth_sign_request(self, url):
    """Signs any request using OAuth. Mandatory before any request of access upon OAuth-protected data"""
    oauth_request = oauth2.Request.from_consumer_and_token(self.consumer, self.token, 'GET', url)
    oauth_request.sign_request(oauth2.SignatureMethod_PLAINTEXT(), self.consumer, self.token)
    request = urllib2.Request(url)
    for header, value in oauth_request.to_header().items():
      request.add_header(header, value)
    return request

  def make_oauth_credentials(self, jsoncreds):
    """Creates the OAuth-related classes for credentials (used for every signed request)"""
    self.consumer = oauth2.Consumer(jsoncreds['consumer_key'], jsoncreds['consumer_secret'])
    self.token = oauth2.Token(jsoncreds['token'], jsoncreds['token_secret'])

  def oauth_get_access_token(self):
    """Aquire an OAuth access token for the given user credentials."""    
    #get hostname
    description = 'Ubuntu One @ %s' % platform.node()
    # Issue a new access token for the user.
    request = urllib2.Request('https://login.ubuntu.com/api/1.0/authentications?' + 
                              urllib.urlencode({'ws.op': 'authenticate', 'token_name': description}))
    request.add_header('Accept', 'application/json')
    request.add_header('Authorization', 'Basic %s' % base64.b64encode('%s:%s' % (self.email, self.passwd)))
    try:
      response = urllib2.urlopen(request)
    except urllib2.HTTPError as httpe:
      if httpe.code == 401: # Unauthorized
        raise AuthenticationFailure("Error 401: Bad email address or password")
      else:
        raise
    credentials = json.load(response)
    return credentials

  def authenticate(self, credentials):
    """Makes first signed request using new OAuth credentials"""
    self.make_oauth_credentials(credentials)
    # Tell Ubuntu One about the new OAuth token.
    get_tokens_url = ('https://one.ubuntu.com/oauth/sso-finished-so-get-tokens/')
    request = self.oauth_sign_request(get_tokens_url)
    response = urllib2.urlopen(request)

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
    except IOError as e:
      # if file doesn't exist
      response = raw_input("Credentials file is unreadable or doesn't exist.\nWould you like to input your U1 credentials instead (will attempt storing the credentials in " + U1Driver.CREDS_FILE + ") ? [Yn] ")
      if response == '' or response.upper() == 'Y':
        jsoncreds = self.credentials_from_email_and_password()
      else:
        raise AuthenticationFailure("Cannot open credentials file (" + credsfile + ")")
    except ValueError:
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
      self.authenticate(credentials)
    except urllib2.HTTPError as httpe:
      if httpe.code == 403:
        rep = raw_input("Error 403: your credentials may have been revoked. Regenerate it using your email/password ? [Yn] ")
        if rep == '' or rep.upper() == 'Y':
          self.prompt_user_credentials()
          credentials = self.oauth_get_access_token()
        else:
          raise AuthenticationFailure("Error 403: Forbidden (Invalid or revoked credentials)")
      else:
        raise
    jsondata = json.dumps(credentials)
    try:
      with open(creds_file, 'wb') as f:
        f.write(jsondata)
    except IOError:
      raise AuthenticationWarning("Could not save credentials to " + creds_file + " file")

  def logout(self):
    pass
  
  def get(self, file_path):
    pass

  def put(self, file_path):
    pass

  def delete(self, file_path):
    pass

  def list_files(self):
    pass
  
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
          cmds[words[0]]() if words[0] != "get" and words[0] != "put" else cmds[words[0]](words[1])
      except KeyError:
        print "Unknown command:", words[0]
      except IndexError:
        print "This command expects an argument (a file name)."
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
    print "Successfully logged in, but errors occurred (" + aw.what + ")"
  else:
    print "Successfully logged in"
    driver.interactive()
    
if __name__ == "__main__":
  main()
