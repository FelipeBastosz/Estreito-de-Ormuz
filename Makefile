.PHONY: pc1 pc2 build down clean client

# Comandos para o Computador 1
pc1:
	@echo "🚀 Iniciando Brokers 1 e 2, Drones 1-2 e Sensores 1-2..."
	docker-compose up --build broker1 broker2 drone1 drone2 sensor-s1a sensor-s2a sensor-s1b sensor-s2b

# Comandos para o Computador 2
pc2:
	@echo "🚀 Iniciando Brokers 3 e 4, Drones 3-4 e Sensores 3-4..."
	docker-compose up --build broker3 broker4 drone3 drone4 sensor-s3a sensor-s4a sensor-s3b sensor-s4b

# Limpeza total (incluindo volumes de persistência)
clean:
	@echo "🧹 Resetando o sistema e apagando memórias..."
	docker-compose down -v

# Iniciar Cliente CLI (Ex: make client IP=127.0.0.1)
client:
	@docker-compose run --rm client $(IP):9081