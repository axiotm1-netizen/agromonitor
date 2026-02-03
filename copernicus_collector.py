# -*- coding: utf-8 -*-
"""
AgroMonitor Pro - Copernicus Data Collector
Usa APIs de Copernicus/ESA para datos climáticos y satelitales profesionales:
- CDS API: Clima histórico y modelos climáticos
- Sentinel Hub: NDVI/NDWI satelital
- Statistical API: Estadísticas zonales

IMPORTANTE: Sistema de gestión de cuota incluido
- Límite mensual: 10,000 Processing Units y 10,000 Requests
- El sistema rastrea el uso y previene exceder el límite
"""

import os
import json
import cdsapi
from datetime import datetime, timedelta
from sentinelhub import (
    SHConfig, 
    BBox, 
    CRS, 
    DataCollection,
    SentinelHubRequest,
    SentinelHubStatistical,
    MimeType,
    bbox_to_dimensions
)
import db_config
import csv
import requests

# ============================================================
# CONFIGURACIÓN OPENWEATHER
# ============================================================
OWM_API_KEY = "ca45f79113069e3524b4877bebe6e0dd"
OWM_URL = "https://api.openweathermap.org/data/2.5/weather"

# ============================================================
# GESTIÓN DE CUOTA
# ============================================================

QUOTA_FILE = os.path.join(os.path.dirname(__file__), 'quota_tracker.json')

# Estimación de Processing Units por operación
PU_COSTS = {
    'map_rgb': 50,      # Mapa RGB
    'map_ndvi': 50,     # Mapa NDVI coloreado
    'ndvi': 30,         # Valor NDVI
    'ndwi': 30,         # Valor NDWI
    'ndsi': 30,         # Valor NDSI
    'zonal_stats': 40,  # Estadísticas zonales
}

def load_quota():
    """Carga el estado actual de la cuota"""
    if os.path.exists(QUOTA_FILE):
        with open(QUOTA_FILE, 'r') as f:
            quota = json.load(f)
            
        # Reset si es un nuevo mes
        current_month = datetime.now().strftime('%Y-%m')
        if quota.get('current_month') != current_month:
            quota['current_month'] = current_month
            quota['processing_units_used'] = 0
            quota['requests_used'] = 0
            quota['collections_today'] = 0
            save_quota(quota)
            print(f"[QUOTA] Nuevo mes detectado - cuota reseteada")
            
        return quota
    else:
        # Crear archivo por defecto
        quota = {
            "monthly_limit_pu": 10000,
            "monthly_limit_requests": 10000,
            "current_month": datetime.now().strftime('%Y-%m'),
            "processing_units_used": 0,
            "requests_used": 0,
            "last_updated": datetime.now().isoformat(),
            "daily_budget_pu": 300,
            "daily_budget_requests": 300,
            "collections_today": 0,
            "last_collection_date": None
        }
        save_quota(quota)
        return quota

def save_quota(quota):
    """Guarda el estado de la cuota"""
    quota['last_updated'] = datetime.now().isoformat()
    with open(QUOTA_FILE, 'w') as f:
        json.dump(quota, f, indent=4)

def check_quota(operation_type='general', pu_cost=50):
    """
    Verifica si hay cuota disponible antes de hacer una operación
    
    Returns:
        bool: True si hay cuota disponible, False si no
    """
    quota = load_quota()
    
    remaining_pu = quota['monthly_limit_pu'] - quota['processing_units_used']
    remaining_req = quota['monthly_limit_requests'] - quota['requests_used']
    
    if remaining_pu < pu_cost:
        print(f"[QUOTA] ⚠️ Sin cuota de PU suficiente. Restante: {remaining_pu}/{quota['monthly_limit_pu']}")
        return False
    
    if remaining_req < 1:
        print(f"[QUOTA] ⚠️ Sin requests restantes. Restante: {remaining_req}/{quota['monthly_limit_requests']}")
        return False
    
    return True

def use_quota(operation_type, pu_cost=None):
    """Registra el uso de cuota después de una operación exitosa"""
    quota = load_quota()
    
    if pu_cost is None:
        pu_cost = PU_COSTS.get(operation_type, 30)
    
    quota['processing_units_used'] += pu_cost
    quota['requests_used'] += 1
    
    # Actualizar contador diario
    today = datetime.now().strftime('%Y-%m-%d')
    if quota.get('last_collection_date') != today:
        quota['collections_today'] = 1
        quota['last_collection_date'] = today
    else:
        quota['collections_today'] += 1
    
    save_quota(quota)
    
    remaining_pu = quota['monthly_limit_pu'] - quota['processing_units_used']
    print(f"[QUOTA] Usado: {pu_cost} PU | Total mes: {quota['processing_units_used']}/{quota['monthly_limit_pu']} | Restante: {remaining_pu}")

def get_quota_status():
    """Retorna el estado actual de la cuota"""
    quota = load_quota()
    
    remaining_pu = quota['monthly_limit_pu'] - quota['processing_units_used']
    remaining_req = quota['monthly_limit_requests'] - quota['requests_used']
    percent_used = (quota['processing_units_used'] / quota['monthly_limit_pu']) * 100
    
    # Calcular días restantes del mes
    today = datetime.now()
    if today.month == 12:
        next_month = datetime(today.year + 1, 1, 1)
    else:
        next_month = datetime(today.year, today.month + 1, 1)
    days_remaining = (next_month - today).days
    
    # Calcular presupuesto diario recomendado
    safe_daily_pu = remaining_pu // max(days_remaining, 1) if days_remaining > 0 else 0
    
    return {
        'pu_used': quota['processing_units_used'],
        'pu_remaining': remaining_pu,
        'pu_limit': quota['monthly_limit_pu'],
        'requests_used': quota['requests_used'],
        'requests_remaining': remaining_req,
        'percent_used': percent_used,
        'days_remaining': days_remaining,
        'safe_daily_pu': safe_daily_pu,
        'collections_today': quota.get('collections_today', 0)
    }

def print_quota_status():
    """Imprime el estado de la cuota de forma visual"""
    status = get_quota_status()
    
    print("\n" + "="*60)
    print("  [QUOTA] ESTADO DE CUOTA - COPERNICUS DATA SPACE")
    print("="*60)
    print(f"  Processing Units: {status['pu_used']:,} / {status['pu_limit']:,} ({status['percent_used']:.1f}%)")
    print(f"  Requests:         {status['requests_used']:,} / {status['pu_limit']:,}")
    print(f"  Restante:         {status['pu_remaining']:,} PU")
    print(f"  Dias del mes:     {status['days_remaining']} restantes")
    print(f"  Presupuesto/dia:  ~{status['safe_daily_pu']} PU recomendado")
    print(f"  Colecciones hoy:  {status['collections_today']}")
    
    # Barra de progreso visual
    bar_length = 40
    filled = int(bar_length * status['percent_used'] / 100)
    bar = "#" * filled + "-" * (bar_length - filled)
    print(f"\n  [{bar}] {status['percent_used']:.1f}%")
    print("="*60 + "\n")

# ============================================================
# CONFIGURACIÓN
# ============================================================

# Coordenadas del área de monitoreo (Veraguas, Panamá)
# Polígono: {"type":"Polygon","coordinates":[[[-81.196969,8.435314],[-81.196969,8.448622],[-81.183665,8.448622],[-81.183665,8.435314],[-81.196969,8.435314]]]}
FARM_COORDS = {
    "lat": 8.441968,  # Centro
    "lon": -81.190317,  # Centro
    "bbox": [-81.196969, 8.435314, -81.183665, 8.448622],  # [min_lon, min_lat, max_lon, max_lat]
    "polygon": {
        "type": "Polygon",
        "coordinates": [[
            [-81.196969, 8.435314],
            [-81.196969, 8.448622],
            [-81.183665, 8.448622],
            [-81.183665, 8.435314],
            [-81.196969, 8.435314]
        ]]
    }
}

# Cargar configuración para Copernicus Data Space
def load_sentinel_config():
    """Carga configuración de Copernicus Data Space (dataspace.copernicus.eu)"""
    config = SHConfig()
    
    # URLs para Copernicus Data Space Ecosystem (CDSE)
    config.sh_base_url = "https://sh.dataspace.copernicus.eu"
    config.sh_auth_base_url = "https://identity.dataspace.copernicus.eu"
    config.sh_token_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    
    # Intentar cargar desde archivo
    config_file = os.path.join(os.path.dirname(__file__), 'sentinel_config.json')
    if os.path.exists(config_file):
        with open(config_file, 'r') as f:
            sentinel_creds = json.load(f)
            config.sh_client_id = sentinel_creds.get('client_id', '')
            config.sh_client_secret = sentinel_creds.get('client_secret', '')
    
    # O desde variables de entorno
    if os.environ.get('SH_CLIENT_ID'):
        config.sh_client_id = os.environ['SH_CLIENT_ID']
        config.sh_client_secret = os.environ['SH_CLIENT_SECRET']
    
    # Guardar configuración
    config.save("cdse")
    
    return config

# DataCollection para CDSE (Copernicus Data Space Ecosystem)
def get_cdse_sentinel2_collection():
    """Retorna la DataCollection correcta para CDSE"""
    return DataCollection.SENTINEL2_L2A.define_from(
        name="s2l2a_cdse",
        service_url="https://sh.dataspace.copernicus.eu"
    )

# ============================================================
# MAPAS SATELITALES
# ============================================================

# Evalscript para imagen RGB True Color
RGB_EVALSCRIPT = """
//VERSION=3
function setup() {
    return {
        input: ["B04", "B03", "B02", "dataMask"],
        output: { bands: 4 }
    };
}

function evaluatePixel(sample) {
    return [2.5 * sample.B04, 2.5 * sample.B03, 2.5 * sample.B02, sample.dataMask];
}
"""

# Evalscript para NDVI coloreado (verde-amarillo-rojo)
NDVI_COLOR_EVALSCRIPT = """
//VERSION=3
function setup() {
    return {
        input: ["B04", "B08", "dataMask"],
        output: { bands: 4 }
    };
}

function evaluatePixel(sample) {
    let ndvi = (sample.B08 - sample.B04) / (sample.B08 + sample.B04);
    
    // Colormap: rojo (bajo) -> amarillo -> verde (alto)
    let r, g, b;
    if (ndvi < 0) {
        r = 0.5; g = 0.5; b = 0.5; // gris para agua/nubes
    } else if (ndvi < 0.2) {
        r = 0.8; g = 0.2; b = 0.1; // rojo - suelo/vegetación pobre
    } else if (ndvi < 0.4) {
        r = 0.9; g = 0.6; b = 0.1; // naranja
    } else if (ndvi < 0.6) {
        r = 0.9; g = 0.9; b = 0.2; // amarillo
    } else if (ndvi < 0.8) {
        r = 0.4; g = 0.8; b = 0.2; // verde claro
    } else {
        r = 0.1; g = 0.6; b = 0.1; // verde oscuro - vegetación densa
    }
    
    return [r, g, b, sample.dataMask];
}
"""

def get_satellite_map(map_type='rgb', start_date=None, end_date=None):
    """
    Genera un mapa satelital de la finca
    
    Args:
        map_type: 'rgb' para True Color, 'ndvi' para NDVI coloreado
        start_date: Fecha inicio
        end_date: Fecha fin
    
    Returns:
        Ruta al archivo PNG guardado
    """
    config = load_sentinel_config()
    
    if not config.sh_client_id:
        print("[WARN] Sentinel Hub no configurado.")
        return None
    
    if not end_date:
        end_date = datetime.now()
    if not start_date:
        start_date = end_date - timedelta(days=30)
    
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    print(f"[Sentinel Hub] Generando mapa {map_type.upper()}...")
    
    try:
        bbox = BBox(bbox=FARM_COORDS['bbox'], crs=CRS.WGS84)
        resolution = 10  # metros
        size = bbox_to_dimensions(bbox, resolution=resolution)
        
        # Aumentar resolución para mejor visualización
        size = (size[0] * 2, size[1] * 2)
        
        evalscript = RGB_EVALSCRIPT if map_type == 'rgb' else NDVI_COLOR_EVALSCRIPT
        cdse_collection = get_cdse_sentinel2_collection()
        
        request = SentinelHubRequest(
            evalscript=evalscript,
            input_data=[
                SentinelHubRequest.input_data(
                    data_collection=cdse_collection,
                    time_interval=(start_date, end_date),
                    mosaicking_order='leastCC'
                )
            ],
            responses=[
                SentinelHubRequest.output_response('default', MimeType.PNG)
            ],
            bbox=bbox,
            size=size,
            config=config
        )
        
        data = request.get_data()
        
        if data and len(data) > 0:
            from PIL import Image
            import numpy as np
            
            img_array = data[0]
            
            # Guardar imagen
            os.makedirs('data/maps', exist_ok=True)
            output_file = f"data/maps/farm_{map_type}_{end_date.strftime('%Y%m%d')}.png"
            
            if img_array.shape[-1] == 4:
                img = Image.fromarray(img_array, 'RGBA')
            else:
                img = Image.fromarray(img_array)
            
            img.save(output_file)
            print(f"[OK] Mapa guardado: {output_file}")
            return output_file
        
    except Exception as e:
        print(f"[ERROR] Generación de mapa: {e}")
        return None

# ============================================================
# OPENWEATHERMAP API - CLIMA ACTUAL
# ============================================================

def get_weather_data():
    """Obtiene datos del clima actual via OpenWeatherMap"""
    print(f"\n[OpenWeather] Consultando clima actual...")
    try:
        params = {
            'lat': FARM_COORDS['lat'],
            'lon': FARM_COORDS['lon'],
            'appid': OWM_API_KEY,
            'units': 'metric',
            'lang': 'es'
        }
        
        response = requests.get(OWM_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        weather = {
            'temp': data['main']['temp'],
            'temp_min': data['main']['temp_min'],
            'temp_max': data['main']['temp_max'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'wind_speed': data['wind']['speed'],
            'wind_deg': data['wind'].get('deg', 0),
            'description': data['weather'][0]['description'],
            'icon': data['weather'][0]['icon'],
            'clouds': data['clouds']['all']
        }
        
        print(f"[OK] Clima actual: {weather['temp']}°C, {weather['description']}")
        return weather
        
    except Exception as e:
        print(f"[ERROR] OpenWeather API: {e}")
        return None

# ============================================================
# CDS API - CLIMA HISTÓRICO
# ============================================================

def get_climate_data_cds(start_date=None, end_date=None):
    """
    Obtiene datos climáticos históricos de ERA5 via CDS API
    
    Datos disponibles:
    - Temperatura (2m)
    - Precipitación total
    - Humedad relativa
    - Viento
    """
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f"[CDS] Solicitando datos ERA5 del {start_date} al {end_date}...")
    
    try:
        c = cdsapi.Client()
        
        # Definir la solicitud para ERA5-Land (resolución horaria)
        result = c.retrieve(
            'reanalysis-era5-land',
            {
                'product_type': 'reanalysis',
                'variable': [
                    '2m_temperature',
                    '2m_dewpoint_temperature',
                    'total_precipitation',
                    '10m_u_component_of_wind',
                    '10m_v_component_of_wind',
                    'soil_temperature_level_1',
                    'volumetric_soil_water_layer_1'
                ],
                'year': start_date[:4],
                'month': start_date[5:7],
                'day': [str(i).zfill(2) for i in range(1, 32)],
                'time': ['12:00'],
                'area': [
                    FARM_COORDS['lat'] + 0.1,  # North
                    FARM_COORDS['lon'] - 0.1,  # West
                    FARM_COORDS['lat'] - 0.1,  # South
                    FARM_COORDS['lon'] + 0.1   # East
                ],
                'format': 'netcdf'
            }
        )
        
        # Guardar archivo
        output_file = f"data/era5_climate_{start_date[:7]}.nc"
        os.makedirs('data', exist_ok=True)
        result.download(output_file)
        
        print(f"[OK] Datos climáticos guardados en {output_file}")
        return output_file
        
    except Exception as e:
        print(f"[ERROR] CDS API: {e}")
        return None

# ============================================================
# SENTINEL HUB - NDVI/NDWI
# ============================================================

# Evalscript para calcular NDVI
NDVI_EVALSCRIPT = """
//VERSION=3
function setup() {
    return {
        input: [{
            bands: ["B04", "B08", "dataMask"]
        }],
        output: [{
            id: "ndvi",
            bands: 1,
            sampleType: "FLOAT32"
        }]
    };
}

function evaluatePixel(sample) {
    let ndvi = (sample.B08 - sample.B04) / (sample.B08 + sample.B04);
    return [ndvi];
}
"""

# Evalscript para calcular NDWI
NDWI_EVALSCRIPT = """
//VERSION=3
function setup() {
    return {
        input: [{
            bands: ["B03", "B08", "dataMask"]
        }],
        output: [{
            id: "ndwi",
            bands: 1,
            sampleType: "FLOAT32"
        }]
    };
}

function evaluatePixel(sample) {
    let ndwi = (sample.B03 - sample.B08) / (sample.B03 + sample.B08);
    return [ndwi];
}
"""

# Evalscript para calcular NDSI (Normalized Difference Soil Index)
# Útil para detectar suelo expuesto vs vegetación
# NDSI = (SWIR - NIR) / (SWIR + NIR) usando B11 (SWIR) y B08 (NIR)
NDSI_EVALSCRIPT = """
//VERSION=3
function setup() {
    return {
        input: [{
            bands: ["B08", "B11", "dataMask"]
        }],
        output: [{
            id: "ndsi",
            bands: 1,
            sampleType: "FLOAT32"
        }]
    };
}

function evaluatePixel(sample) {
    // NDSI: valores positivos = suelo, negativos = vegetación
    let ndsi = (sample.B11 - sample.B08) / (sample.B11 + sample.B08);
    return [ndsi];
}
"""

def get_ndvi_sentinel(start_date=None, end_date=None):
    """
    Obtiene NDVI de Sentinel-2 via Sentinel Hub Processing API
    """
    config = load_sentinel_config()
    
    if not config.sh_client_id:
        print("[WARN] Sentinel Hub no configurado. Configura sentinel_config.json")
        return None
    
    if not end_date:
        end_date = datetime.now()
    if not start_date:
        start_date = end_date - timedelta(days=30)
    
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    print(f"[Sentinel Hub] Calculando NDVI del {start_date.date()} al {end_date.date()}...")
    
    try:
        # Definir bounding box
        bbox = BBox(bbox=FARM_COORDS['bbox'], crs=CRS.WGS84)
        resolution = 10  # metros
        size = bbox_to_dimensions(bbox, resolution=resolution)
        
        # Crear solicitud usando DataCollection de CDSE
        cdse_collection = get_cdse_sentinel2_collection()
        
        request = SentinelHubRequest(
            evalscript=NDVI_EVALSCRIPT,
            input_data=[
                SentinelHubRequest.input_data(
                    data_collection=cdse_collection,
                    time_interval=(start_date, end_date),
                    mosaicking_order='leastCC'  # Menor nubosidad
                )
            ],
            responses=[
                SentinelHubRequest.output_response('ndvi', MimeType.TIFF)
            ],
            bbox=bbox,
            size=size,
            config=config
        )
        
        # Ejecutar
        data = request.get_data()
        
        if data and len(data) > 0:
            ndvi_array = data[0]
            mean_ndvi = float(ndvi_array[ndvi_array != 0].mean())
            print(f"[OK] NDVI promedio: {mean_ndvi:.4f}")
            return {
                'ndvi_mean': mean_ndvi,
                'ndvi_min': float(ndvi_array[ndvi_array != 0].min()),
                'ndvi_max': float(ndvi_array[ndvi_array != 0].max()),
                'date': end_date.isoformat()
            }
        
    except Exception as e:
        print(f"[ERROR] Sentinel Hub NDVI: {e}")
        return None

def get_ndwi_sentinel(start_date=None, end_date=None):
    """
    Obtiene NDWI de Sentinel-2 via Sentinel Hub Processing API
    """
    config = load_sentinel_config()
    
    if not config.sh_client_id:
        print("[WARN] Sentinel Hub no configurado.")
        return None
    
    if not end_date:
        end_date = datetime.now()
    if not start_date:
        start_date = end_date - timedelta(days=30)
    
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    print(f"[Sentinel Hub] Calculando NDWI del {start_date.date()} al {end_date.date()}...")
    
    try:
        bbox = BBox(bbox=FARM_COORDS['bbox'], crs=CRS.WGS84)
        resolution = 10
        size = bbox_to_dimensions(bbox, resolution=resolution)
        
        # Usar DataCollection de CDSE
        cdse_collection = get_cdse_sentinel2_collection()
        
        request = SentinelHubRequest(
            evalscript=NDWI_EVALSCRIPT,
            input_data=[
                SentinelHubRequest.input_data(
                    data_collection=cdse_collection,
                    time_interval=(start_date, end_date),
                    mosaicking_order='leastCC'
                )
            ],
            responses=[
                SentinelHubRequest.output_response('ndwi', MimeType.TIFF)
            ],
            bbox=bbox,
            size=size,
            config=config
        )
        
        data = request.get_data()
        
        if data and len(data) > 0:
            ndwi_array = data[0]
            mean_ndwi = float(ndwi_array[ndwi_array != 0].mean())
            print(f"[OK] NDWI promedio: {mean_ndwi:.4f}")
            return {
                'ndwi_mean': mean_ndwi,
                'ndwi_min': float(ndwi_array[ndwi_array != 0].min()),
                'ndwi_max': float(ndwi_array[ndwi_array != 0].max()),
                'date': end_date.isoformat()
            }
        
    except Exception as e:
        print(f"[ERROR] Sentinel Hub NDWI: {e}")
        return None

def get_ndsi_sentinel(start_date=None, end_date=None):
    """
    Obtiene NDSI (Normalized Difference Soil Index) de Sentinel-2
    NDSI positivo = suelo expuesto, negativo = vegetación
    """
    config = load_sentinel_config()
    
    if not config.sh_client_id:
        print("[WARN] Sentinel Hub no configurado.")
        return None
    
    if not end_date:
        end_date = datetime.now()
    if not start_date:
        start_date = end_date - timedelta(days=30)
    
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    print(f"[Sentinel Hub] Calculando NDSI del {start_date.date()} al {end_date.date()}...")
    
    try:
        bbox = BBox(bbox=FARM_COORDS['bbox'], crs=CRS.WGS84)
        resolution = 10
        size = bbox_to_dimensions(bbox, resolution=resolution)
        
        cdse_collection = get_cdse_sentinel2_collection()
        
        request = SentinelHubRequest(
            evalscript=NDSI_EVALSCRIPT,
            input_data=[
                SentinelHubRequest.input_data(
                    data_collection=cdse_collection,
                    time_interval=(start_date, end_date),
                    mosaicking_order='leastCC'
                )
            ],
            responses=[
                SentinelHubRequest.output_response('ndsi', MimeType.TIFF)
            ],
            bbox=bbox,
            size=size,
            config=config
        )
        
        data = request.get_data()
        
        if data and len(data) > 0:
            ndsi_array = data[0]
            valid_data = ndsi_array[ndsi_array != 0]
            if len(valid_data) > 0:
                mean_ndsi = float(valid_data.mean())
                print(f"[OK] NDSI promedio: {mean_ndsi:.4f}")
                return {
                    'ndsi_mean': mean_ndsi,
                    'ndsi_min': float(valid_data.min()),
                    'ndsi_max': float(valid_data.max()),
                    'date': end_date.isoformat(),
                    'interpretation': 'Suelo expuesto' if mean_ndsi > 0 else 'Vegetación cubriendo'
                }
        
    except Exception as e:
        print(f"[ERROR] Sentinel Hub NDSI: {e}")
        return None

# ============================================================
# STATISTICAL API - ESTADÍSTICAS ZONALES
# ============================================================

def get_zonal_statistics(start_date=None, end_date=None):
    """
    Obtiene estadísticas zonales sin descargar datos completos
    usando Sentinel Hub Statistical API
    """
    config = load_sentinel_config()
    
    if not config.sh_client_id:
        print("[WARN] Sentinel Hub no configurado.")
        return None
    
    if not end_date:
        end_date = datetime.now()
    if not start_date:
        start_date = end_date - timedelta(days=30)
    
    print(f"[Statistical API] Obteniendo estadísticas zonales...")
    
    try:
        # Geometría del polígono de la finca
        geometry = {
            "type": "Polygon",
            "coordinates": [[
                [FARM_COORDS['bbox'][0], FARM_COORDS['bbox'][1]],
                [FARM_COORDS['bbox'][2], FARM_COORDS['bbox'][1]],
                [FARM_COORDS['bbox'][2], FARM_COORDS['bbox'][3]],
                [FARM_COORDS['bbox'][0], FARM_COORDS['bbox'][3]],
                [FARM_COORDS['bbox'][0], FARM_COORDS['bbox'][1]]
            ]]
        }
        
        # Evalscript para estadísticas
        evalscript = """
        //VERSION=3
        function setup() {
            return {
                input: [{ bands: ["B04", "B08", "dataMask"] }],
                output: [
                    { id: "ndvi", bands: 1, sampleType: "FLOAT32" },
                    { id: "dataMask", bands: 1 }
                ]
            };
        }
        
        function evaluatePixel(samples) {
            let ndvi = (samples.B08 - samples.B04) / (samples.B08 + samples.B04);
            return {
                ndvi: [ndvi],
                dataMask: [samples.dataMask]
            };
        }
        """
        
        request = SentinelHubStatistical(
            aggregation=SentinelHubStatistical.aggregation(
                evalscript=evalscript,
                time_interval=(start_date, end_date),
                aggregation_interval='P1D',  # Diario
                size=(100, 100)
            ),
            input_data=[
                SentinelHubStatistical.input_data(
                    DataCollection.SENTINEL2_L2A,
                    maxcc=0.3
                )
            ],
            geometry=geometry,
            config=config
        )
        
        stats = request.get_data()
        print(f"[OK] Estadísticas obtenidas para {len(stats)} fechas")
        return stats
        
    except Exception as e:
        print(f"[ERROR] Statistical API: {e}")
        return None

# ============================================================
# PERSISTENCIA DE DATOS
# ============================================================

def save_results(results):
    """Guarda los resultados en CSV y Base de Datos"""
    if not results:
        print("[WARN] No hay resultados para guardar")
        return

    timestamp = datetime.now()
    date_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    
    # 2. Guardar en CSV local
    csv_file = 'data/copernicus_history.csv'
    os.makedirs('data', exist_ok=True)
    
    # Preparar fila
    row = {
        'timestamp': date_str,
        'polygon_id': 'los_valles_veraguas',
        'ndvi_mean': results.get('ndvi', {}).get('ndvi_mean', ''),
        'ndwi_mean': results.get('ndwi', {}).get('ndwi_mean', ''),
        'ndsi_mean': results.get('ndsi', {}).get('ndsi_mean', ''),
        'ndsi_interp': results.get('ndsi', {}).get('interpretation', ''),
        'map_rgb': results.get('map_rgb', ''),
        'map_ndvi': results.get('map_ndvi', ''),
        'temp_c': results.get('weather', {}).get('temp', ''),
        'temp_min_c': results.get('weather', {}).get('temp_min', ''),
        'temp_max_c': results.get('weather', {}).get('temp_max', ''),
        'humidity': results.get('weather', {}).get('humidity', ''),
        'weather_desc': results.get('weather', {}).get('description', '')
    }
    
    file_exists = os.path.isfile(csv_file)
    with open(csv_file, 'a', newline='', encoding='utf-8') as f:
        # Nota: Si el archivo ya existe y tiene otros encabezados, esto podría ser un problema
        # Idealmente verificar headers, pero por simplicidad agregamos al final
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)
    print(f"[OK] Datos guardados en CSV: {csv_file}")
    
    # 2. Guardar en Base de Datos (Neon PostgreSQL)
    try:
        conn = db_config.get_connection()
        if conn:
            cur = conn.cursor()
            
            # Insertar Clima (OpenWeather)
            if 'weather' in results and results['weather']:
                w = results['weather']
                cur.execute("""
                    INSERT INTO weather_data
                    (polygon_id, temperature_c, temp_min_c, temp_max_c, humidity_percent, pressure_hpa, wind_speed_ms, wind_deg, clouds_percent, weather_main, weather_description)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    'los_valles_veraguas',
                    w.get('temp'),
                    w.get('temp_min'),
                    w.get('temp_max'),
                    w.get('humidity'),
                    w.get('pressure'),
                    w.get('wind_speed'),
                    w.get('wind_deg'),

                    w.get('clouds'),
                    'OpenWeather',
                    w.get('description')
                ))
                print("[OK] Clima guardado en BD")
            
            # Insertar NDVI/NDWI/NDSI
            if 'ndvi' in results or 'ndwi' in results or 'ndsi' in results:
                cur.execute("""
                    INSERT INTO ndvi_data 
                    (polygon_id, ndvi_mean, ndvi_min, ndvi_max, ndwi_mean, ndsi_mean, ndsi_interpretation, cloud_coverage)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    'los_valles_veraguas',
                    results.get('ndvi', {}).get('ndvi_mean'),
                    results.get('ndvi', {}).get('ndvi_min'),
                    results.get('ndvi', {}).get('ndvi_max'),
                    results.get('ndwi', {}).get('ndwi_mean'),
                    results.get('ndsi', {}).get('ndsi_mean'),
                    results.get('ndsi', {}).get('interpretation'),
                    0.0 # Cloud coverage placeholder
                ))
            
            conn.commit()
            cur.close()
            conn.close()
            print("[OK] Datos guardados en Base de Datos Neon")
    except Exception as e:
        print(f"[ERROR] Guardando en BD: {e}")

# ============================================================
# MAIN
# ============================================================

def collect_all_copernicus_data(mode='normal'):
    """
    Recolecta todos los datos de las APIs de Copernicus
    
    Args:
        mode: 'normal' (todos los datos ~220 PU) 
              'economic' (solo índices ~90 PU)
              'minimal' (solo NDVI ~30 PU)
    """
    print("\n" + "="*60)
    print("  AGROMONITOR PRO - COPERNICUS DATA COLLECTOR")
    print("="*60)
    print(f"  Fecha/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Ubicación: Veraguas, Panamá")
    print(f"  Modo: {mode.upper()}")
    print("="*60)
    
    # Mostrar estado de cuota primero
    print_quota_status()
    
    # Verificar cuota general antes de empezar
    estimated_pu = {'normal': 220, 'economic': 90, 'minimal': 30}
    if not check_quota('collection', estimated_pu.get(mode, 220)):
        print("\n⚠️ No hay suficiente cuota para esta operación.")
        print("   Intenta con mode='minimal' o espera al próximo mes.")
        return None
    
    results = {}
    
    # 0. Clima Actual (OpenWeatherMap) - No consume cuota Copernicus
    print("\n[0/5] Obteniendo clima actual (OpenWeather)...")
    results['weather'] = get_weather_data()
    
    if mode == 'normal':
        # MODO COMPLETO: ~220 PU
        
        # 1. Mapa RGB satelital (~50 PU)
        print("\n[1/5] Generando mapa RGB satelital...")
        if check_quota('map_rgb', PU_COSTS['map_rgb']):
            results['map_rgb'] = get_satellite_map('rgb')
            if results['map_rgb']:
                use_quota('map_rgb')
        
        # 2. Mapa NDVI coloreado (~50 PU)
        print("\n[2/5] Generando mapa NDVI coloreado...")
        if check_quota('map_ndvi', PU_COSTS['map_ndvi']):
            results['map_ndvi'] = get_satellite_map('ndvi')
            if results['map_ndvi']:
                use_quota('map_ndvi')
        
        # 3. NDVI valor numérico (~30 PU)
        print("\n[3/5] Obteniendo NDVI de Sentinel-2...")
        if check_quota('ndvi', PU_COSTS['ndvi']):
            results['ndvi'] = get_ndvi_sentinel()
            if results['ndvi']:
                use_quota('ndvi')
        
        # 4. NDWI valor numérico (~30 PU)
        print("\n[4/5] Obteniendo NDWI de Sentinel-2...")
        if check_quota('ndwi', PU_COSTS['ndwi']):
            results['ndwi'] = get_ndwi_sentinel()
            if results['ndwi']:
                use_quota('ndwi')
        
        # 5. NDSI valor numérico (~30 PU)
        print("\n[5/5] Obteniendo NDSI de Sentinel-2...")
        if check_quota('ndsi', PU_COSTS['ndsi']):
            results['ndsi'] = get_ndsi_sentinel()
            if results['ndsi']:
                use_quota('ndsi')
    
    elif mode == 'economic':
        # MODO ECONÓMICO: Solo índices ~90 PU
        
        print("\n[1/3] Obteniendo NDVI de Sentinel-2...")
        if check_quota('ndvi', PU_COSTS['ndvi']):
            results['ndvi'] = get_ndvi_sentinel()
            if results['ndvi']:
                use_quota('ndvi')
        
        print("\n[2/3] Obteniendo NDWI de Sentinel-2...")
        if check_quota('ndwi', PU_COSTS['ndwi']):
            results['ndwi'] = get_ndwi_sentinel()
            if results['ndwi']:
                use_quota('ndwi')
        
        print("\n[3/3] Obteniendo NDSI de Sentinel-2...")
        if check_quota('ndsi', PU_COSTS['ndsi']):
            results['ndsi'] = get_ndsi_sentinel()
            if results['ndsi']:
                use_quota('ndsi')
    
    else:  # minimal
        # MODO MÍNIMO: Solo NDVI ~30 PU
        
        print("\n[1/1] Obteniendo NDVI de Sentinel-2...")
        if check_quota('ndvi', PU_COSTS['ndvi']):
            results['ndvi'] = get_ndvi_sentinel()
            if results['ndvi']:
                use_quota('ndvi')
    
    # Resumen
    print("\n" + "="*60)
    print("  RESUMEN")
    print("="*60)
    if results.get('map_rgb'):
        print(f"  Mapa RGB: {results['map_rgb']}")
    if results.get('map_ndvi'):
        print(f"  Mapa NDVI: {results['map_ndvi']}")
    if results.get('ndvi'):
        print(f"  NDVI promedio: {results['ndvi']['ndvi_mean']:.4f}")
    if results.get('ndwi'):
        print(f"  NDWI promedio: {results['ndwi']['ndwi_mean']:.4f}")
    if results.get('ndsi'):
        print(f"  NDSI promedio: {results['ndsi']['ndsi_mean']:.4f} ({results['ndsi']['interpretation']})")
    
    # Estado final de cuota
    print_quota_status()
    
    # Guardar resultados
    save_results(results)
    
    return results

def collect_economic():
    """Atajo para recolección económica (~90 PU)"""
    return collect_all_copernicus_data(mode='economic')

def collect_minimal():
    """Atajo para recolección mínima (~30 PU)"""
    return collect_all_copernicus_data(mode='minimal')

if __name__ == "__main__":
    import sys
    
    # Permitir seleccionar modo desde línea de comandos
    mode = 'economic'  # Por defecto económico para ahorrar cuota
    if len(sys.argv) > 1:
        if sys.argv[1] in ['normal', 'economic', 'minimal', 'status']:
            if sys.argv[1] == 'status':
                print_quota_status()
            else:
                mode = sys.argv[1]
                collect_all_copernicus_data(mode=mode)
    else:
        # Sin argumentos, mostrar status y preguntar
        print_quota_status()
        print("Uso: python copernicus_collector.py [normal|economic|minimal|status]")
        print("  normal   - Todos los datos (~220 PU)")
        print("  economic - Solo índices (~90 PU)")
        print("  minimal  - Solo NDVI (~30 PU)")
        print("  status   - Ver estado de cuota")
        print("\nEjecutando modo económico por defecto...")
        collect_all_copernicus_data(mode='normal')
