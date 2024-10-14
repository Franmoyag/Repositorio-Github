
import React, { useState } from 'react';
import { View, Text, Button, StyleSheet, TextInput } from 'react-native';
import { StackNavigationProp } from '@react-navigation/stack';
import { RootStackParamList } from './types';

type InventoryScreenNavigationProp = StackNavigationProp<RootStackParamList, 'Inventory'>;

type Props = {
  navigation: InventoryScreenNavigationProp;
};

const InventoryScreen: React.FC<Props> = ({ navigation }) => {
  const [warehouse, setWarehouse] = useState('');
  const [date, setDate] = useState('');

  const handleStartInventory = () => {
    if (warehouse && date) {
      navigation.navigate('Capture', { warehouse, date });
    } else {
      alert('Por favor, seleccione una bodega y una fecha');
    }
  };

  return (
    <View style={styles.container}>
      <Text style={styles.title}>Iniciar Inventario</Text>
      <Text>Selecciona Bodega:</Text>
      <Button title="Casa Matriz" onPress={() => setWarehouse('Casa Matriz')} />
      <Button title="Concepción" onPress={() => setWarehouse('Concepción')} />
      <Text>Ingresa Fecha de Inventario:</Text>
      <TextInput
        style={styles.input}
        placeholder="DD/MM/YYYY"
        value={date}
        onChangeText={setDate}
      />
      <Button title="Iniciar" onPress={handleStartInventory} />
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    justifyContent: 'center',
    padding: 20,
  },
  title: {
    fontSize: 24,
    textAlign: 'center',
    marginBottom: 20,
  },
  input: {
    borderWidth: 1,
    borderColor: '#ccc',
    padding: 10,
    marginBottom: 10,
  },
});

export default InventoryScreen;
