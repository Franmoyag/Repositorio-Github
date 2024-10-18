import React from "react";
import {
  View,
  Text,
  FlatList,
  TouchableOpacity,
  StyleSheet,
} from "react-native";
import { useAppStore } from "../store/useAppStore";
import { StackNavigationProp } from "@react-navigation/stack";
import { RootStackParamList } from "./types"; // Importa los tipos de rutas



type InventariosRealizadosNavigationProp = StackNavigationProp<
  RootStackParamList,
  "InventoriesList"
>;

type Props = {
  navigation: InventariosRealizadosNavigationProp;
};

const InventariosRealizados: React.FC<Props> = ({ navigation }) => {
  const inventories = useAppStore((state) => state.inventories);

  const validateInventory = (code: any) => {
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  
    if (uuidRegex.test(code)) {
      return true;
    } else {
      return false;
    }

  };

  return (
    <View style={styles.container}>
      <Text style={styles.header}>Inventarios Realizados</Text>
      {inventories.length > 0 ? (
        <FlatList
        data={inventories}
        keyExtractor={(item, index) => `${item.id}-${index}`} // Asegura que cada clave sea única
        renderItem={({ item }) => {
          if (validateInventory(item.id)) {
            return (
              <TouchableOpacity
                style={styles.inventoryItem}
                onPress={() => {
                  console.log("Inventario seleccionado:", item);
                  console.log("Productos pasando a detalles:", item.products);
      
                  navigation.navigate("InventoryDetails", {
                    id: item.id,
                    date: item.date || "Fecha no Disponible",
                    warehouse: item.warehouse,
                    products: item.products || [], // Pasar los productos correctamente
                  });
                }}
              >
                <Text style={styles.date}>Fecha: {item.date}</Text>
                <Text style={styles.warehouse}>Almacén: {item.warehouse}</Text>
              </TouchableOpacity>
            );
          } else {
            return null; // Retorna null si validateInventory es falso
          }
        }}
      />
      ) : (
        <Text>No hay inventarios realizados aún.</Text>
      )}
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    padding: 20,
    justifyContent: "center",
    alignItems: "center",
  },
  header: {
    fontSize: 24,
    fontWeight: "bold",
    marginBottom: 20,
  },
  inventoryItem: {
    padding: 10,
    marginVertical: 8,
    backgroundColor: "#f9f9f9",
    borderRadius: 5,
    width: "100%",
  },
  title: {
    fontSize: 18,
    fontWeight: "bold",
  },
  date: {
    fontSize: 14,
    color: "#666",
  },
  warehouse: {
    fontSize: 14,
    color: "#333",
  },
});

export default InventariosRealizados;
