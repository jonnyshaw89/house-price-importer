run:
	pipenv run python importer.py

docker_build:
	docker build -t jonnyshaw89/house-price-importer:latest .

docker_deploy:
	docker push jonnyshaw89/house-price-importer:latest
