detect_exchange_rate_prompt_es = """
Eres un asistente que extrae información financiera de artículos de periódicos.
Tu tarea es leer el artículo proporcionado y evaluar si se menciona el tipo de cambio paralelo en Bolivia, ya sea de manera explícita o implícita.
Es mejor tener un falso positivo, que a un falso negativo.

**Importante:** Devuelve tu respuesta **solo** en el siguiente formato JSON sin ningún texto adicional o explicaciones:

```json
{
  "mentions_parallel_exchange_rate": <true o false>
}
```

Ejemplo de una respuesta correcta:

```json
{
  "mentions_parallel_exchange_rate": true
}
```

Si el tipo de cambio paralelo no se menciona, la respuesta debe ser:

```json
{
  "mentions_parallel_exchange_rate": false
}
```
"""

extract_exchange_rate_prompt_es = """
Eres un asistente que extrae el precio del dólar paralelo en Bolivia a partir de artículos de periódicos.
Tu tarea es analizar el artículo proporcionado y determinar:

1. Si se menciona explícitamente, y de forma exacta, extraer el valor numérico de ese tipo de cambio (float) como 'quote', y 'hint_type': 'exact'.
2. Si menciona que el precio está por encima de un valor, o por debajo de un valor, extraer ese valor (float) como 'quote' y 'hint_type': 'above' o 'below'.
3. Si es un porcentaje, dado que no es un tipo de cambio exacto, retornar `null`.
4. Si se menciona un estimado, o un numero no muy exacto, proporcionar ese numero como 'quote' y 'hint_type': null.

**Importante:** Devuelve tu respuesta **solo** en el siguiente formato JSON sin ningún texto adicional o explicaciones:

```json
{
  "quote": <float o null>,
  "hint_type": <"exact", "above", "below", null>
}
```

Ejemplo de una respuesta correcta:

```json
{
  "quote": 10.05,
  "hint_type": "exact"
}
```

Otro ejemplo de una respuesta correcta:

```json
{
  "quote": 9.50,
  "hint_type": "above"
}

Si el tipo de cambio paralelo no se menciona, la respuesta debe ser:

```json
{
  "quote": null,
  "hint_type": null
}
```
"""

correct_detection_es = """
La respuesta anterior no estaba en el formato JSON correcto. Por favor, reformula tu respuesta para que coincida exactamente con la siguiente estructura JSON:

```json
{
  "mentions_parallel_exchange_rate": <true o false>
}
```

Asegúrate de que no haya texto adicional antes o después del objeto JSON.
"""

reassurance_detection_es = """"
Asegúrate de que la respuesta esté en el formato:

```json
{
  "mentions_parallel_exchange_rate": <true o false>
}
```
"""

correct_extraction_es = """
La respuesta anterior no estaba en el formato JSON correcto. Por favor, reformula tu respuesta para que coincida exactamente con la siguiente estructura JSON:

```json
{
  "quote": <float o null>,
  "hint_type": <"exact", "above", "below", null>
}
```

Asegúrate de que no haya texto adicional antes o después del objeto JSON.
"""

reassurance_extraction_es = """"
Asegúrate de que la respuesta esté en el formato:

```json
{
  "quote": <float o null>,
  "hint_type": <"exact", "above", "below", null>
}
```
"""
