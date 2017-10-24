package mapper;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import model.CensoInfo;

import java.util.HashMap;
import java.util.Map;

public class Query1Mapper implements Mapper<String, CensoInfo, String, Integer> {

    @Override
    public void map(String key, CensoInfo value, Context<String, Integer> context) {
        final Map<String, String> provinceToRegion = provinceToRegion();
        String region = provinceToRegion.get(value.getProvince());
        context.emit(region, 1);
    }

    private Map<String, String> provinceToRegion() {
        Map<String, String> map = new HashMap<>();
        // Pueden faltar
        map.put("Ciudad Autónoma de Buenos Aires", "Región Buenos Aires");
        map.put("Buenos Aires", "Región Buenos Aires");
        map.put("Corrientes", "Región del Norte Grande Argentino");
        map.put("Chaco", "Región del Norte Grande Argentino");
        map.put("Neuquén", "Región Patagonica");
        map.put("Chubut", "Región Patagonica");
        map.put("Córdoba", "Región Centro");
        map.put("Entre Ríos", "Región Centro");
        map.put("Tucumán", "Región del Norte Grande Argentino");
        map.put("Tierra del Fuego", "Región Patagonica");
        map.put("Santa Fe", "Región Centro");
        map.put("Formosa", "Región del Norte Grande Argentino");
        map.put("La Pampa", "Región Patagonica");
        map.put("Misiones", "Región del Norte Grande Argentino");
        map.put("Río negro", "Región Patagonica");
        map.put("Salta", "Región del Norte Grande Argentino");
        map.put("Jujuy", "Región del Norte Grande Argentino");
        map.put("Santiago del Estero", "Región del Norte Grande Argentino");
        map.put("San Luis", "Region del Nuevo Cuyo");
        map.put("Mendoza", "Region del Nuevo Cuyo");
        map.put("Catamarca", "Región del Norte Grande Argentino");
        map.put("La Rioja", "Region del Nuevo Cuyo");
        map.put("San Juan", "Region del Nuevo Cuyo");
        map.put("Santa Cruz", "Región Patagonica");
        return map;
    }

}
