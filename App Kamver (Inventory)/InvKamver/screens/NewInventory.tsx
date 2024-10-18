// Muestra datos capturados.

import React, { useState } from 'react';
import { StackNavigationProp } from '@react-navigation/stack';
import { RootStackParamList } from './types';
import { StyleSheet, View, Text, ScrollView, TextInput } from 'react-native';
import { FAB } from 'react-native-paper';
import Icon from 'react-native-vector-icons/Ionicons';
import { useAppStore } from '../store/useAppStore';
import useProductStore from '../store/useAppStore';
import uuidv4 from 'uuid/v4';


type NewInventoryNavigationProp = StackNavigationProp<RootStackParamList, 'NewInventory' | 'Capture'>;

type Props = { navigation: NewInventoryNavigationProp };

const NewInventory: React.FC<Props> = ({ navigation }) => {
  const user = useAppStore((state) => state.user);
  const inventories = useAppStore((state) => state.inventories);
  const { products, addProduct, updateProduct } = useProductStore();
  const addInventory = useAppStore((state) => state.addInventory);

  // Controlar el cambio de cantidad en los productos usando el índice
  const handleChangeQuantity = (index, quantity) => {
    // Actualizar el producto usando el índice en lugar del código
    updateProduct(index, { quantity: Number(quantity) });
    console.log (quantity)
  };

  const handleNewCapture = () => {
    // Navegar a la pantalla de captura
    navigation.navigate('Capture');
  };

  const handleNewProductCapture = (newProduct) => {
    // Verificar si el producto ya existe en la lista
    const productExists = products.some((item) => item.code === newProduct.code);

    if (productExists) {
      alert('El producto ya ha sido capturado antes, pero será añadido de todas formas.');
    }

    // Agregar el producto a la lista, incluso si es un duplicado
    addProduct(newProduct);
  };


  // Función para guardar el inventario en el store
  const saveInventory = () => {
    const newInventory = {
      id: uuidv4(), // Generar un ID único para el inventario
      warehouse: 'Casa Matriz', // Cambia esto por el almacén correcto
      date: new Date().toLocaleDateString(), // Fecha actual
      products: [...products], // Lista de productos capturados
    };
  
    console.log('Guardando inventario:', newInventory); // Verificar si los productos están siendo guardados
  
    addInventory(newInventory); // Guardar el inventario en el store
    navigation.navigate('InventoriesList'); // Ir a la lista de inventarios
  };

  return (
    <View style={styles.mainContainer}>
      <ScrollView contentContainerStyle={styles.scrollViewContainer}>
        <View style={styles.container}>
          <Text style={styles.header}>Usuario: {user?.email ? user.email : 'Sin usuario'}</Text>  
          {inventories.length > 0 && (
            <Text style={styles.subHeader}>Inventario ID: {inventories[inventories.length - 1].id}</Text>
          )}
          <Text style={styles.subHeader}>Datos Escaneados:</Text>
          <View style={{ flex: 1 }}>
            {products.map((item, index) => (
              <View key={index} style={{ flexDirection: 'row', justifyContent: 'space-between' }}>
                <Text style={{ margin: 10 }}>{item.code}</Text>
                <Text style={{ margin: 10 }}>{item.title}</Text>
                <TextInput
                  style={styles.input}
                  value={String(item.quantity)}  // Convertir a string para el TextInput
                  keyboardType="numeric"
                  placeholder="Cantidad"
                  onChangeText={(quantity) => handleChangeQuantity(index, quantity)}  // Actualiza la cantidad usando el índice
                />
                <Text style={{ margin: 10 }}>{item.unit}</Text>
              </View>
            ))}
          </View>
        </View>
      </ScrollView>

      {/* FAB para Nueva Captura */}
      <FAB
        icon={(props) => <Icon {...props} name="camera-outline" size={20} />}
        label="Nueva Captura"
        style={styles.fab}
        onPress={() => navigation.navigate('Capture')}
      />

      <FAB
        icon={(props) => <Icon {...props} name="save-outline" size={20} />}
        label="Guardar Inventario"
        style={[styles.fab, { bottom: 80, left: 16 }]} // Ajustamos para que no se superponga con "Nueva Captura"
        onPress={saveInventory} 
      />
    </View>
  );
};

const styles = StyleSheet.create({
  mainContainer: {
    flex: 1,
  },
  scrollViewContainer: {
    paddingBottom: 80,  // Deja un espacio suficiente para que el FAB no bloquee el contenido
  },
  container: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
  },
  header: {
    fontSize: 20,
    fontWeight: 'bold',
    marginBottom: 10,
  },
  subHeader: {
    fontSize: 16,
    marginBottom: 20,
  },
  fab: {
    position: 'absolute',
    right: 16,  // Separación desde el borde derecho
    bottom: 20,  // Separación desde el borde inferior
  },
  input:{
    height: 30,
    width: 100,
    backgroundColor: '#ffffff',
    borderWidth: 0.3,
    borderColor: 'black',
    padding: 5,
  }
});

export default NewInventory;
