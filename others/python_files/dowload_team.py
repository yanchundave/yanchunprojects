import urllib.request

url = 'https://demoforthedaves.atlassian.net/wiki/spaces/D/pages/1979154738/Data+Analytics+Team+Weekly'
response = urllib.request.urlopen(url)
data = response.read()
text = data.decode('utf-8')

print(text)