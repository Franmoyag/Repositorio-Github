// NewInventory.tsx

import React from 'react';
import { StackNavigationProp } from '@react-navigation/stack';
import { RootStackParamList } from './types';
import { StyleSheet, View, Text, FlatList, ScrollView, TextInput } from 'react-native';
import { FAB } from 'react-native-paper';
import Icon from 'react-native-vector-icons/Ionicons';
import { useAppStore } from '../store/useAppStore';

import useProductStore from '../store/useAppStore';

type NewInventoryNavigationProp = StackNavigationProp<RootStackParamList, 'NewInventory' | 'Capture'>;

type Props = { navigation: NewInventoryNavigationProp };

const NewInventory: React.FC<Props> = ({ navigation }) => {
  const user = useAppStore((state) => state.user);
  const inventories = useAppStore((state) => state.inventories);
  const scannedData = useAppStore((state) => state.scannedData); // Obtener los datos escaneados

  const { products, clearProducts } = useProductStore();

  console.log(products);

  const handleNewCapture = () => {
    navigation.navigate('Capture');
  };


  const handleChangeQuantity = (quantity) => {
    //let quantity = event.target.value;
    console.log(quantity); 
  }

  return (
    <ScrollView>
    <View style={styles.container}>
      <Text style={styles.header}>Usuario: {user?.email}</Text>
      {inventories.length > 0 && (
        <Text style={styles.subHeader}>Inventario ID: {inventories[inventories.length - 1].id}</Text>
      )}
      <Text style={styles.subHeader}>Datos Escaneados:</Text>
      <View style={{flex: 1}}>
        {
          products.map(item => {
            return (
              <View style={{flexDirection: 'row', justifyContent: 'space-between'}}>
                <Text style={{margin: 10}}>{item.code}</Text>
                <Text style={{margin: 10}}>{item.title}</Text>
                <TextInput  style={{height: 30, width: 100, backgroundColor: '#FFF', borderWidth: 1, borderColor: '#f3f3f3'}} 
                  value={item.quantity} 
                  keyboardType="numeric"
                  placeholder='Cantidad' 
                  onChangeText={(quantity) => handleChangeQuantity(quantity)}
                />
                <Text style={{margin: 10}}>{item.unit}</Text>
              </View>
            )
          })
        }

      </View>
      <FAB
        icon={(props) => <Icon {...props} name="camera-outline" size={20} />}
        label="Nueva Captura"
        style={styles.fab}
        onPress={handleNewCapture}
      />
    </View>
    </ScrollView>
  );
};

const styles = StyleSheet.create({
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
  scannedItem: {
    fontSize: 14,
    color: '#333',
    marginBottom: 5,
  },
  noData: {
    fontSize: 14,
    color: '#999',
  },
  fab: {
    position: 'absolute',
    margin: 16,
    right: 0,
    bottom: 20,
  },
});

export default NewInventory;
