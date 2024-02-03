aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin 249959970268.dkr.ecr.eu-west-1.amazonaws.com

docker build -t bfsp_scraper .

docker tag bfsp_scraper:latest 249959970268.dkr.ecr.eu-west-1.amazonaws.com/bfsp_scraper:latest

docker push 249959970268.dkr.ecr.eu-west-1.amazonaws.com/bfsp_scraper:latest
