// Muestra datos capturados.

import React, { useState } from 'react';
import { StackNavigationProp } from '@react-navigation/stack';
import { RootStackParamList } from './types';
import { StyleSheet, View, Text, ScrollView, TextInput } from 'react-native';
import { FAB } from 'react-native-paper';
import Icon from 'react-native-vector-icons/Ionicons';
import { useAppStore } from '../store/useAppStore';
import useProductStore from '../store/useAppStore';

type NewInventoryNavigationProp = StackNavigationProp<RootStackParamList, 'NewInventory' | 'Capture'>;

type Props = { navigation: NewInventoryNavigationProp };

const NewInventory: React.FC<Props> = ({ navigation }) => {
  const user = useAppStore((state) => state.user);
  const inventories = useAppStore((state) => state.inventories);
  const { products, clearProducts, addProduct, updateProduct } = useProductStore();

  // Controlar la cantidad a nivel de productos
  const handleChangeQuantity = (code, quantity) => {
    updateProduct(code, quantity);
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
            {products.map((item) => (
              <View key={item.code} style={{ flexDirection: 'row', justifyContent: 'space-between' }}>
                <Text style={{ margin: 10 }}>{item.code}</Text>
                <Text style={{ margin: 10 }}>{item.title}</Text>
                <TextInput
                  style={{ height: 30, width: 100, backgroundColor: '#FFF', borderWidth: 1, borderColor: '#f3f3f3' }}
                  value={String(item.quantity)}  // Asegúrate de que es un string
                  keyboardType="numeric"
                  placeholder="Cantidad"
                  onChangeText={(quantity) => handleChangeQuantity(item.code, quantity)}  // Actualiza la cantidad
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
});

export default NewInventory;
