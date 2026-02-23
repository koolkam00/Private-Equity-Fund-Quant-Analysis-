import urllib.request
try:
    with urllib.request.urlopen("http://127.0.0.1:5000/dashboard") as response:
        pass
    print("Dashboard loaded.")
except Exception as e:
    print(e)
