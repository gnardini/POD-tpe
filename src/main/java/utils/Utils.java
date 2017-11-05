package utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Utils {

    public static Map<String, String> regionMap;

    public static Map<String, String> provinceToRegion() {
        if (regionMap == null) {
            regionMap = new HashMap<>();
            regionMap.put("Ciudad Autónoma de Buenos Aires", "Región Buenos Aires");
            regionMap.put("Buenos Aires", "Región Buenos Aires");
            regionMap.put("Corrientes", "Región del Norte Grande Argentino");
            regionMap.put("Chaco", "Región del Norte Grande Argentino");
            regionMap.put("Neuquén", "Región Patagonica");
            regionMap.put("Chubut", "Región Patagonica");
            regionMap.put("Córdoba", "Región Centro");
            regionMap.put("Entre Ríos", "Región Centro");
            regionMap.put("Tucumán", "Región del Norte Grande Argentino");
            regionMap.put("Tierra del Fuego", "Región Patagonica");
            regionMap.put("Santa Fe", "Región Centro");
            regionMap.put("Formosa", "Región del Norte Grande Argentino");
            regionMap.put("La Pampa", "Región Patagonica");
            regionMap.put("Misiones", "Región del Norte Grande Argentino");
            regionMap.put("Río negro", "Región Patagonica");
            regionMap.put("Salta", "Región del Norte Grande Argentino");
            regionMap.put("Jujuy", "Región del Norte Grande Argentino");
            regionMap.put("Santiago del Estero", "Región del Norte Grande Argentino");
            regionMap.put("San Luis", "Region del Nuevo Cuyo");
            regionMap.put("Mendoza", "Region del Nuevo Cuyo");
            regionMap.put("Catamarca", "Región del Norte Grande Argentino");
            regionMap.put("La Rioja", "Region del Nuevo Cuyo");
            regionMap.put("San Juan", "Region del Nuevo Cuyo");
            regionMap.put("Santa Cruz", "Región Patagonica");
            regionMap = Collections.unmodifiableMap(regionMap);
        }
        return regionMap;
    }
}
