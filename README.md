# Development

Install `serverless`:
```
sudo npm install -g osls
serverless plugin install -n serverless-scaleway-functions
```

Create a python venv and install dev dependencies:
```
python3.12 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements-dev.txt
```

Deploy the function:
```
./bin/deploy.sh
```
