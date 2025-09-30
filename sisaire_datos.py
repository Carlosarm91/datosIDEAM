import requests
from bs4 import BeautifulSoup
import re
import datetime
import json
import time
from requests.exceptions import RequestException
from supabase import create_client, Client
    
# 🚀 Configuración Supabase
url = "https://nldidqrguinqubknbevg.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5sZGlkcXJndWlucXVia25iZXZnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTg0ODMzNzYsImV4cCI6MjA3NDA1OTM3Nn0.c57Qsr2d4OwA4IcMKsIR_3iUHv5yrafWnbuI-x3uxHc"
supabase: Client = create_client(url, key)


def hacer_post_con_reintento(url, data, headers, max_reintentos=3, delay=5):
    for intento in range(max_reintentos):
        try:
            return session.post(url, data=data, headers=headers, timeout=15)
        except RequestException as e:
            print(f"⚠️ Intento {intento+1} fallido: {e}")
            time.sleep(delay)
    raise Exception(f"❌ Fallaron los {max_reintentos} intentos para POST a {url}")

# 📌 Logs
def log_to_db(level: str, message):
    try:
        message = json.dumps(message, ensure_ascii=False)  # Asegura que sea string
        supabase.table("logs_sisaire").insert({
            "nivel": level,
            "mensaje": message
        }).execute()
    except Exception as e:
        print(f"❌ Error guardando log en DB: {e}")

# 🌐 Sesión web
session = requests.Session()
url_scrap = "http://sisaire.ideam.gov.co/ideam-sisaire-web/consultas.xhtml"
headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Faces-Request": "partial/ajax",
    "User-Agent": "Mozilla/5.0"
}

try:
    # 1️⃣ Obtener ViewState
    resp = session.get(url_scrap, timeout=10)
    soup = BeautifulSoup(resp.text, "html.parser")
    viewstate = soup.find("input", {"name": "javax.faces.ViewState"})["value"]

    # 2️⃣ Cargar datos desde Supabase
    estaciones = supabase.table("estaciones_sisaire").select("id, codigo_estacion, municipio_id").execute().data
    municipios = supabase.table("municipios_sisaire").select("id, codigo_municipio, departamento_id").execute().data
    departamentos = supabase.table("departamentos_sisaire").select("id, codigo_departamento").execute().data

    mun_map = {m["id"]: m for m in municipios}
    dep_map = {d["id"]: d for d in departamentos}

    for est in reversed(estaciones):
        estacion_id = est["id"]
        estacion_code = est["codigo_estacion"]
        mun = mun_map.get(est["municipio_id"])
        if not mun:
            continue
        dep = dep_map.get(mun["departamento_id"])

        dep_code = dep["codigo_departamento"]
        mun_code = mun["codigo_municipio"]

        # 3️⃣ Seleccionar departamento
        session.post(url_scrap, data={
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": "filtroForm:departamentoSel",
            "javax.faces.partial.execute": "filtroForm:departamentoSel",
            "javax.faces.partial.render": "filtroForm msgs-globales",
            "javax.faces.behavior.event": "valueChange",
            "javax.faces.partial.event": "change",
            "filtroForm": "filtroForm",
            "filtroForm:departamentoSel_input": dep_code,
            "javax.faces.ViewState": viewstate,
        }, headers=headers)

        # 4️⃣ Obtener fechas disponibles
        resp_fecha = session.post(url_scrap, data={
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": "filtroForm:j_idt70",
            "javax.faces.partial.execute": "@all",
            "javax.faces.partial.render": "filtroForm:contaminanteSel filtroForm:labelFIniLimite filtroForm:labelFFinLimite msgs-globales",
            "filtroForm:j_idt70": "filtroForm:j_idt70",
            "filtroForm": "filtroForm",
            "filtroForm:departamentoSel_input": dep_code,
            "filtroForm:tipoSel_input": "2",
            "filtroForm:municipioSel_input": mun_code,
            "filtroForm:estacionesSel": estacion_code,
            "javax.faces.ViewState": viewstate,
        }, headers=headers)

        soup_fecha = BeautifulSoup(resp_fecha.text, "xml")
        updates = soup_fecha.find_all("update")
        fecha_desde, fecha_hasta = None, None

        for upd in updates:
            texto = upd.get_text(" ", strip=True)
            match_desde = re.search(r"Mediciones disponibles desde\s+(\d{4}-\d{2}-\d{2})", texto)
            match_hasta = re.search(r"Mediciones disponibles hasta\s+(\d{4}-\d{2}-\d{2})", texto)
            if match_desde: fecha_desde = match_desde.group(1)
            if match_hasta: fecha_hasta = match_hasta.group(1)

        if not fecha_desde or not fecha_hasta:
            log_to_db("WARN", f"No fechas para estación {estacion_code}")
            continue

        d_ini = datetime.date.fromisoformat(fecha_desde)
        d_fin = datetime.date.fromisoformat(fecha_hasta)

        # 5️⃣ Iterar por cada día
        for d in (d_ini + datetime.timedelta(days=i) for i in range((d_fin - d_ini).days + 1)):
            
            print(f"   📅 Procesando {d.isoformat()} en estación {estacion_id}", flush=True)
            # ⚠️ Verificar si ya hay registros para ese día, estación y municipio
            try:
                existe = supabase.table("datos_sisaire").select("id") \
                    .eq("estacion_id", estacion_id) \
                    .eq("municipio_id", est["municipio_id"]) \
                    .eq("fecha_observacion", d.isoformat()) \
                    .limit(1) \
                    .execute()

                if existe.data:
                    continue
            except Exception as e:
                log_to_db("ERROR", f"Error verificando existencia previa para {d.isoformat()}, estación {estacion_id}: {e}")
                print(f"❌ Error verificando existencia: {e}")
                continue

            # 👇 Solo se ejecuta si no existe
            resp_consulta = hacer_post_con_reintento(url_scrap, data={
                "javax.faces.partial.ajax": "true",
                "javax.faces.source": "filtroForm:btnConsultar",
                "javax.faces.partial.execute": "@all",
                "javax.faces.partial.render": "contenedorResultados msgs-globales",
                "filtroForm:btnConsultar": "filtroForm:btnConsultar",
                "filtroForm": "filtroForm",
                "filtroForm:departamentoSel_input": dep_code,
                "filtroForm:municipioSel_input": mun_code,
                "filtroForm:estacionesSel": estacion_code,
                "filtroForm:fechaIni_input": d.isoformat(),
                "filtroForm:fechaFin_input": d.isoformat(),
                "filtroForm:tipoSel_input": "2",
                "javax.faces.ViewState": viewstate,
            }, headers=headers)

            soup_post = BeautifulSoup(resp_consulta.text, "xml")
            upd = soup_post.find("update", {"id": "contenedorResultados"})
            if not upd:
                continue

            soup_table = BeautifulSoup(upd.text, "html.parser")
            headers_tabla = [th.get_text(strip=True).upper() for th in soup_table.find_all("th")]
            rows = soup_table.find_all("tr")

            for row in rows:
                cols = [td.get_text(strip=True) for td in row.find_all("td")]
                if not cols:
                    continue

                record = {
                    "estacion_id": estacion_id,
                    "municipio_id": est["municipio_id"],
                    "fecha_observacion": d.isoformat(),
                    "pm10": None,
                    "pm25": None,
                    "no2": None,
                    "o3": None,
                }

                for i, h in enumerate(headers_tabla):
                    if i >= len(cols): continue
                    val = cols[i].replace(",", ".")
                    try:
                        val = float(val)
                    except ValueError:
                        val = None

                    if h.startswith("PM10"):
                        record["pm10"] = val
                    elif h.startswith("PM2.5") or h.startswith("PM25"):
                        record["pm25"] = val
                    elif h.startswith("NO2"):
                        record["no2"] = val
                    elif h.startswith("O3"):
                        record["o3"] = val

                if any([record["pm10"], record["pm25"], record["no2"], record["o3"]]):
                    try:
                        # ✅ Verifica si ya existe un registro con misma estación, municipio y fecha
                        existe = supabase.table("datos_sisaire").select("id") \
                            .eq("estacion_id", record["estacion_id"]) \
                            .eq("municipio_id", record["municipio_id"]) \
                            .eq("fecha_observacion", record["fecha_observacion"]) \
                            .execute()

                        if existe.data:
                            continue  # No insertes

                        # 🔽 Insertar si no existe
                        response = supabase.table("datos_sisaire").insert(record).execute()
                        print("✅ Insertado:", response.data)
                        log_to_db("INFO", f"Insertado en datos_sisaire: {response.data[0]}")
                    except Exception as e:
                        log_to_db("ERROR", f"Error insertando: {record}, Error: {e}")

except Exception as e:
    log_to_db("ERROR", f"Error general: {e}")
    print(f"❌ Error general: {e}")
