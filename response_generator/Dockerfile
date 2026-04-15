FROM python:3.11-slim

WORKDIR /app

# Copiamos primero el requirements para aprovechar la caché de Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el resto del código del servicio
COPY . .

# Exponemos el puerto para FastAPI
EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]