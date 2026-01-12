# üö≤ Bicimadness
Aplicaci®Æn web para el an®¢lisis de datos de BiciMAD basada en una arquitectura distribuida con frontend en React, backend en FastAPI y una base de datos DuckDB almacenada de forma privada. El proyecto est®¢ desplegado utilizando ®≤nicamente servicios gratuitos y con control de acceso al frontend.
---
## √çndice

1. Descripci®Æn general  
2. URLs del proyecto  
3. Arquitectura  
4. Frontend  
5. Backend / API  
6. Base de datos  
7. Almacenamiento (Cloudflare R2)  
8. Seguridad y control de acceso  

---

## **1. Descripci®Æn general**

Bicimadness es una aplicaci®Æn web que permite consultar y analizar datos hist®Æricos del sistema BiciMAD. El frontend ofrece visualizaciones e interacci®Æn con el usuario, mientras que el backend expone una API REST que consulta una base de datos DuckDB de gran tama?o (~300 MB).

Los datos no se incluyen en el repositorio y se gestionan de forma privada mediante almacenamiento externo.

---

## **2. URLs del proyecto**

Frontend (protegido con autenticaci®Æn):
https://tgisbicimad.jjimenezfernan.workers.dev/

Backend / API:
https://bicimadness.onrender.com/

---

## **3. Arquitectura**

La arquitectura del proyecto es la siguiente:

- El usuario accede al frontend desplegado en Cloudflare Workers.
- Cloudflare Access controla qui√©n puede acceder a la aplicaci®Æn.
- El frontend realiza peticiones HTTPS a la API.
- La API est®¢ desplegada en Render usando FastAPI.
- El backend descarga el archivo DuckDB desde Cloudflare R2 al arrancar.
- DuckDB se abre en modo de solo lectura para realizar consultas.

---

## **4. Frontend**

- Tecnolog®™a: React
- Hosting: Cloudflare Workers (workers.dev)
- Acceso: restringido mediante Cloudflare Access
- M√©todo de autenticaci®Æn: One-time PIN por email
- El c®Ædigo del frontend no contiene datos sensibles
- No es necesario modificar el frontend para la autenticaci®Æn

---

## **5. Backend / API**

- Framework: FastAPI (Python)
- Hosting: Render (plan gratuito)
- Tipo: API REST
- Funcionalidades principales:
  - Consultas por estaci®Æn y fecha
  - Series temporales
  - Res®≤menes mensuales y anuales
  - Exportaci®Æn de datos en formato Excel (XLSX)

El backend se inicia descargando la base de datos desde Cloudflare R2 y despu√©s levanta el servidor FastAPI.

---

## **6. Base de datos**

- Motor: DuckDB
- Tama?o aproximado: 300 MB
- Modo de uso: solo lectura
- El archivo no se encuentra en el repositorio
- El backend abre la base de datos localmente tras descargarla

---

## **7. Almacenamiento (Cloudflare R2)**

- Servicio: Cloudflare R2 (S3-compatible)
- Bucket: bicimadness
- Archivo: bicimad.duckdb
- Acceso: privado
- Credenciales: solo lectura para el backend
- No hay costes de salida de datos (egress)

---

## **8. Seguridad y control de acceso**

### Frontend

- Protegido con Cloudflare Access
- Solo los emails permitidos en la pol®™tica pueden acceder
- Autenticaci®Æn mediante c®Ædigo enviado por email

### Backend

- La API es p®≤blicamente accesible a nivel de red
- El acceso efectivo est®¢ controlado por el frontend protegido
- La base de datos es de solo lectura
- No existen endpoints de escritura destructiva

---

## **9. Costes**

El proyecto utiliza ®≤nicamente planes gratuitos:

- Cloudflare Workers: gratuito
- Cloudflare Access: gratuito
