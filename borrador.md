Actúa como un Senior Data Engineer especializado en análisis cuantitativo e ingeniería de datos financieros para el mercado argentino.
Necesitamos continuar/modificar un pipeline de análisis de conversaciones de marketing en Instagram.

### Contexto del Proyecto
1. **Objetivo:** Analizar transcripciones de Instagram DMs para responder preguntas de negocio (como dolores de clientes, categorizaciones, conversión, etc.).
2. **Entrada de Datos:** 
   - Ruta: `D:/DatosDeMercado/marketing_data/instagram_20260802/processed/instagram_audio_transcripts.csv`
   - Encoding: `latin-1`
   - Campos clave: `conversation_id`, `sender_name`, `timestamp_ms`, `full_text`
3. **Lógica de Filtro ("FLUJO"):**
   - Buscamos conversaciones que el cliente (no Tomas Cueva) inicie mandando la palabra "FLUJO", "Flujo" o "flujo".
   - El primer mensaje del cliente en orden cronológico (`timestamp_ms`) debe empezar con "FLUJO" (case-insensitive).
   - El sender del asesor financiero contiene la palabra "Cueva".

### Stack Técnico Validado
- **Entorno Virtual:** `.\venv\Scripts\python` (PowerShell)
- **API SDK:** `google-genai` (Instalada y validada. ¡NO usar el deprecated `google.generativeai`!).
- **API Key:** Variable `GEMINI_API_KEY` en el archivo `.env`.
- **Modelo Gemini Validado (Paid Tier):** `gemini-flash-latest` (Confirmado con acceso activo y cuota a demanda).
- **Configuración de Batch:** Procesar de a 20 conversaciones por llamada con 3 segundos de sleep entre batches es óptimo.

### Archivos del Proyecto
Los scripts estables y la exploración previa se encuentran en la ruta:
`c:\Users\tomas\white_finance\scripts\layers\marketing\analisis de conversaciones`
- [analisis_dolores_flujo.py](file:///c:/Users/tomas/white_finance/scripts/layers/marketing/analisis%20de%20conversaciones/analisis_dolores_flujo.py) -> Script principal que extrae y normaliza los dolores usando Gemini.
- [flujo_explore2.py](file:///c:/Users/tomas/white_finance/scripts/layers/marketing/analisis%20de%20conversaciones/flujo_explore2.py) -> Explorador inicial del dataset.
- [test_modelo_pago.py](file:///c:/Users/tomas/white_finance/scripts/layers/marketing/analisis%20de%20conversaciones/test_modelo_pago.py) -> Validador de modelos de la API de Google.

### Nueva Tarea de Negocio
Objetivo: Analizar transcripciones de Instagram DMs para responder a la pregunta ¿qué tipo de seguimiento más me funcionó para continuar conversaciones que me llevaron a mandar el link de Calendly para agendar una llamada?.
Notas adicionales: Un seguimiento es un mensaje que le envío a una persona despues de que la misma no me haya respondido por un lapso de tiempo mayor a 20 minutos o me haya dejado en visto un mensaje anterior. A modo de saber si la persona perdió el chat, le molestó un mensaje o no tiene ganas de seguir conversando.