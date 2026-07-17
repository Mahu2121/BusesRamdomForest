# BusesRamdomForest

Modelo de predicción basado en **Random Forest** para estimar tiempos de llegada de autobuses en tiempo real, integrado con Firebase como backend.

Desarrollado como motor de predicción para la aplicación **[Confibus](https://github.com/Mahu2121/ConfiBus.git)**.

---

##  ¿Qué hace?

- Entrena un modelo Random Forest con datos históricos de autobuses
- Exporta el modelo como `.pkl` y lo despliega en **Firebase Cloud Functions**
- Guarda predicciones pre-calculadas en **Firestore** por parada
- Expone una **API HTTP** que devuelve el resultado al instante cuando el usuario pulsa una parada en Confibus

## ️ Arquitectura

```
Datos en tiempo real
        ↓
Modelo Random Forest (.pkl)
        ↓
Firebase Cloud Functions (Python)
        ↓
Firestore → predicciones por parada_id
        ↓
API HTTP → Confibus (app móvil)
```

##  Estructura del proyecto

```
BusesRandomForest/
├── functions/
│   ├── main.py               # Cloud Function + API HTTP
│   └── requirements.txt      # Dependencias Python
├── firestore.rules           # Reglas de seguridad
├── firestore.indexes.json    # Índices de Firestore
├── firebase.json             # Configuración Firebase
└── pyproject.toml            # Configuración del proyecto Python
```

## ️ Stack

| Tecnología | Uso |
|---|---|
| Python 3.12 | Entrenamiento del modelo y Cloud Functions |
| scikit-learn | Random Forest |
| Firebase Cloud Functions | API y lógica backend |
| Firestore | Almacenamiento de predicciones |
| Firebase CLI | Despliegue |

##  Despliegue

```bash
# Instalar Firebase CLI
npm install -g firebase-tools

# Login y configuración
firebase login
firebase init

# Desplegar funciones
firebase deploy --only functions
```


## Actualizar modelo
```
1. Entrenas en local (cuando tengas datos nuevos)
cd entrenamiento/
python train_model.py        

2. Copias el modelo a functions/
cp model.pkl ../functions/

3. Despliegas a Firebase
cd ..
firebase deploy --only functions
```

##  Proyecto relacionado

Este repositorio es el backend de predicción de **[Confibus](https://github.com/Mahu2121/ConfiBus.git)**, una aplicación móvil para consultar llegadas de autobuses en tiempo real.

---
