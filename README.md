# 🚲 Bicimadness
Aplicación web para el análisis de datos de BiciMAD basada en una arquitectura distribuida con frontend en React, backend en FastAPI y una base de datos DuckDB almacenada de forma privada. El proyecto está desplegado utilizando únicamente servicios gratuitos y con control de acceso al frontend.

## Índice

1. Descripción general  
2. URLs del proyecto  
3. Arquitectura  
4. Frontend  
5. Backend / API  
6. Base de datos  
7. Almacenamiento (Cloudflare R2)  
8. Seguridad y control de acceso  



## **1. Descripción general**

Bicimadness es una aplicación web que permite consultar y analizar datos históricos del sistema BiciMAD. El frontend ofrece visualizaciones e interacción con el usuario, mientras que el backend expone una API REST que consulta una base de datos DuckDB de gran tamaño (~300 MB).

Los datos no se incluyen en el repositorio y se gestionan de forma privada mediante almacenamiento externo.



## **2. URLs del proyecto**

Frontend (protegido con autenticación):
https://tgisbicimad.jjimenezfernan.workers.dev/

Backend / API:
https://bicimadness.onrender.com/



## **3. Arquitectura**

La arquitectura del proyecto es la siguiente:

- El usuario accede al frontend desplegado en Cloudflare Workers.
- Cloudflare Access controla quién puede acceder a la aplicación.
- El frontend realiza peticiones HTTPS a la API.
- La API está desplegada en Render usando FastAPI.
- El backend descarga el archivo DuckDB desde Cloudflare R2 al arrancar.
- DuckDB se abre en modo de solo lectura para realizar consultas.



## **4. Frontend**

- Tecnología: React
- Hosting: Cloudflare Workers (workers.dev)
- Acceso: restringido mediante Cloudflare Access
- Método de autenticación: One-time PIN por email
- El código del frontend no contiene datos sensibles
- No es necesario modificar el frontend para la autenticación



## **5. Backend / API**

- Framework: FastAPI (Python)
- Hosting: Render (plan gratuito)
- Tipo: API REST
- Funcionalidades principales:
  - Consultas por estación y fecha
  - Series temporales
  - Resúmenes mensuales y anuales
  - Exportación de datos en formato Excel (XLSX)

El backend se inicia descargando la base de datos desde Cloudflare R2 y después levanta el servidor FastAPI.



## **6. Base de datos**

- Motor: DuckDB
- Tamaño aproximado: 300 MB
- Modo de uso: solo lectura
- El archivo no se encuentra en el repositorio
- El backend abre la base de datos localmente tras descargarla



## **7. Almacenamiento (Cloudflare R2)**

- Servicio: Cloudflare R2 (S3-compatible)
- Bucket: bicimadness
- Archivo: bicimad.duckdb
- Acceso: privado
- Credenciales: solo lectura para el backend
- No hay costes de salida de datos (egress)



## **8. Seguridad y control de acceso**

### Frontend

- Protegido con Cloudflare Access
- Solo los emails permitidos en la política pueden acceder
- Autenticación mediante código enviado por email

### Backend

- La API es públicamente accesible a nivel de red
- El acceso efectivo está controlado por el frontend protegido
- La base de datos es de solo lectura
- No existen endpoints de escritura destructiva



## **9. Costes**

El proyecto utiliza únicamente planes gratuitos:

- Cloudflare Workers: gratuito
- Cloudflare Access: gratuito
