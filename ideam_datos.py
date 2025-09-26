from supabase import create_client, Client
import requests

# 🚀 Configuración Supabase
url = "https://nldidqrguinqubknbevg.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5sZGlkcXJndWlucXVia25iZXZnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTg0ODMzNzYsImV4cCI6MjA3NDA1OTM3Nn0.c57Qsr2d4OwA4IcMKsIR_3iUHv5yrafWnbuI-x3uxHc"
supabase: Client = create_client(url, key)

# 📌 Función para logs
def log_event(nivel: str, mensaje: str):
    try:
        supabase.table("logs_sisaire").insert({
            "nivel": nivel,
            "mensaje": mensaje
        }).execute()
    except Exception as e:
        print(f"⚠️ No se pudo guardar log en Supabase: {e}")

# 📌 Sensores que sí queremos
SENSORES_VALIDOS = {
    "VELOCIDAD DEL VIENTO",
    "DIRECCIÓN DEL VIENTO",
    "TEMPERATURA DEL AIRE A 2 m",
    "HUMEDAD DEL SUELO A 1 m Ó MAS",
    "PRECIPITACIÓN"
}

try:
    # 1️⃣ Obtener municipios desde la tabla municipios_ideam
    municipios_resp = supabase.table("municipios_ideam").select("nombre_municipio").execute()

    if not municipios_resp.data:
        log_event("ERROR", "No se encontraron municipios en municipios_ideam")
        raise Exception("Tabla municipios_ideam vacía")

    for row in municipios_resp.data:
        municipio = row["nombre_municipio"]

        try:
            # 2️⃣ Consultar API IDEAM para ese municipio
            api_url = "https://www.datos.gov.co/resource/57sv-p2fu.json"
            params = {
                "municipio": municipio,
                "$limit": 50000,
                "$$app_token": "6dZgRuB9ToR29JIJqndvSvXLe"
            }

            resp = requests.get(api_url, params=params, timeout=20)

            if resp.status_code != 200:
                log_event("ERROR", f"API IDEAM falló para {municipio} (HTTP {resp.status_code})")
                continue

            data = resp.json()

            if not data:
                log_event("INFO", f"Sin datos para {municipio}")
                continue

            # 3️⃣ Filtrar solo sensores válidos y descartar valorobservado = 0
            datos_filtrados = []
            for item in data:
                try:
                    valor = float(item["valorobservado"]) if item.get("valorobservado") else None
                except:
                    valor = None

                if (
                    item.get("descripcionsensor") in SENSORES_VALIDOS
                    and valor is not None
                    and valor != 0
                ):
                    datos_filtrados.append({
                        "descripcionsensor": item["descripcionsensor"],
                        "unidadmedida": item.get("unidadmedida"),
                        "valorobservado": valor,
                        "fechaobservacion": item["fechaobservacion"],
                        "municipio": municipio
                    })

            if not datos_filtrados:
                log_event("INFO", f"Sin sensores válidos o valores distintos de 0 en {municipio}")
                continue

            # 4️⃣ Insertar en la tabla datos_ideam
            for registro in datos_filtrados:
                try:
                    supabase.table("datos_ideam").insert(registro).execute()
                except Exception as e:
                    log_event("ERROR", f"Error insertando dato en {municipio}: {e}")

            log_event("INFO", f"{len(datos_filtrados)} registros insertados para {municipio}")

        except Exception as e:
            log_event("ERROR", f"Error procesando municipio {municipio}: {e}")

except Exception as e:
    log_event("ERROR", f"Error general en ejecución IDEAM datos: {e}")
    print(f"❌ Error general: {e}")
