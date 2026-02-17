python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

wget https://github.com/denoland/deno/releases/download/v2.6.9/deno-x86_64-unknown-linux-gnu.zip
apt install unzip
unzip deno-x86_64-unknown-linux-gnu.zip
chmod +x deno
mv deno /usr/local/bin/
deno --version