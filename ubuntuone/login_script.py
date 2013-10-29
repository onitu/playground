#!/usr/bin/python

import argparse, base64, getpass, oauth2, platform, urllib, urllib2

class AuthenticationFailure(Exception):
  """The provided email address and password were incorrect."""
  def __init__(self, what):
    self.what = "Authentication failure: " + what

class U1Driver:
  BASE_API_PATH = "/api/file_storage/v1"
  CREDENTIALS = '.credentials'

  
  def __init__(self, resource_host="https://edge.one.ubuntu.com", content_host="https://files.one.ubuntu.com"):
    self.email = ''
    self.passwd = ''
    self.resource_host = resource_host
    self.content_host = content_host

  def sign_request(url):
    oauth_request = oauth2.Request.from_consumer_and_token(self.consumer, self.token, 'GET', url)
    oauth_request.sign_request(oauth2.SignatureMethod_PLAINTEXT(), self.consumer, self.token)
    request = urllib2.Request(url)
    for header, value in oauth_request.to_header().items():
      request.add_header(header, value)
    return request

  def oauth_credentials(self, data):
    consumer = oauth2.Consumer(data['consumer_key'], data['consumer_secret'])
    token = oauth2.Token(data['token'], data['token_secret'])
    return consumer, token
    
  def authenticate(self):
    #get hostname
    description = 'Ubuntu One @ %s' % platform.node()
    """Aquire an OAuth access token for the given user."""
    # Issue a new access token for the user.
    request = urllib2.Request('https://login.ubuntu.com/api/1.0/authentications?' + 
                              urllib.urlencode({'ws.op': 'authenticate', 'token_name': description}))
    request.add_header('Accept', 'application/json')
    request.add_header('Authorization', 'Basic %s' % base64.b64encode('%s:%s' % (self.email, self.passwd)))
    try:
      response = urllib2.urlopen(request)
    except urllib2.HTTPError, exc:
      if exc.code == 401: # Unauthorized
        raise AuthenticationFailure("Bad email address or password")
      else:
        raise
    data = json.load(response)
    self.consumer, self.token = oauth_credentials(data)
    # Tell Ubuntu One about the new token.
    get_tokens_url = ('https://one.ubuntu.com/oauth/sso-finished-so-get-tokens/')
    request = sign_request(self.consumer, self.token, get_tokens_url)
    response = urllib2.urlopen(request)
    print response.headers
    print response.read()
    print 'Success'
    return data

  def login(self):
    pass

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

def main():
  driver = U1Driver()

  parser = argparse.ArgumentParser(description="Test Onitu Driver for Ubuntu One", epilog="2013 Onitu")
  parser.add_argument('-p', action='store_true', default=False, dest='prompt', help='Prompts user identification')
  args = parser.parse_args()
  if args.prompt:
    driver.email = raw_input("Ubuntu One email: ")
    driver.passwd = getpass.getpass("Ubuntu One Password for " + driver.email + ": ")
    print driver.email, driver.passwd
    try:
      driver.authenticate()
    except AuthenticationFailure as af:
      print af.what
    else:
      print "Successfully authenticated"


if __name__ == "__main__":
  main()
