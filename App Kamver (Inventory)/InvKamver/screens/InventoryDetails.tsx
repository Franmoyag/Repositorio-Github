import React from "react";
import { View, Text, StyleSheet } from "react-native";
import { RouteProp } from '@react-navigation/native';
import { RootStackParamList } from "./types";


type InventoryDetailsRouteProp = RouteProp<RootStackParamList, 'InventoryDetails'>;

type Props = {
    route: InventoryDetailsRouteProp;
};

const InventoryDetails: React.FC<Props> = ({ route }) => {

    const { id, date, warehouse, products = [] } = route.params;
    
  

    console.log('Productos en detalles:', products); // Agrega este console.log

    return (
        <View style={styles.container}>
            <Text style={styles.title}>Detalles del Inventario</Text>
            <Text>ID: {id}</Text>
            <Text>Fecha: {date}</Text>
            <Text>Almacén: {warehouse}</Text>

            <Text style={styles.subtitle}>Productos:</Text>
            {products.length > 0 ? (
                products.map((product, index) => (
                    <View key={index} style={styles.productItem}>
                        <Text>{product.title} (Cantidad: {product.quantity})</Text>
                    </View>
                ))
            ) : (
                <Text>No hay productos en este inventario.</Text>
            )}
        </View>
    );
};

const styles = StyleSheet.create({
    container: {
        flex: 1,
        padding: 20,
        justifyContent: 'center',
        alignItems: 'center',
    },
    title: {
        fontSize: 24,
        fontWeight: 'bold',
        marginBottom: 20,
    },
    subtitle: {
        fontSize: 18,
        marginTop: 10,
        fontWeight: 'bold',
    },
    productItem: {
        marginVertical: 5,
    },
});

export default InventoryDetails;
