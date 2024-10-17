//Screen Inicio de Inventario

import React, { useState } from 'react';
import { View, Text, Button, StyleSheet, TouchableOpacity } from 'react-native';
import { StackNavigationProp } from '@react-navigation/stack';
import { RootStackParamList } from './types';
import { RadioButton } from 'react-native-paper';
import DateTimePicker from '@react-native-community/datetimepicker';
import { useAppStore, AppState } from '../store/useAppStore';

type InventoryScreenNavigationProp = StackNavigationProp<RootStackParamList, 'Inventory'>;

type Props = {
  navigation: InventoryScreenNavigationProp;
};

const InventoryScreen: React.FC<Props> = ({ navigation }) => {
  const [warehouse, setWarehouse] = useState('Casa Matriz');
  const [date, setDate] = useState(new Date());
  const [showDatePicker, setShowDatePicker] = useState(false);
  const addInventory = useAppStore((state: AppState) => state.addInventory);

  const handleStartInventory = () => {
    if (warehouse && date) {
      addInventory({ id: `${warehouse}-${date.toLocaleDateString()}`, warehouse, date: date.toLocaleDateString() });
      navigation.navigate('NewInventory');
    } else {
      alert('Por favor, seleccione una bodega y una fecha');
    }
  };

  return (
    <View style={styles.container}>
      <Text style={styles.title}>Iniciar Inventario</Text>
      <Text>Selecciona Bodega:</Text>
      <RadioButton.Group onValueChange={newValue => setWarehouse(newValue)} value={warehouse}>
        <View style={styles.radioContainer}>
          <RadioButton value="Casa Matriz" />
          <Text>Casa Matriz</Text>
        </View>
        <View style={styles.radioContainer}>
          <RadioButton value="Concepción" />
          <Text>Concepción</Text>
        </View>
      </RadioButton.Group>
      <Text>Ingresa Fecha de Inventario:</Text>
      <TouchableOpacity style={styles.dateButton} onPress={() => setShowDatePicker(true)}>
        <Text>{date ? date.toLocaleDateString() : 'Selecciona una fecha'}</Text>
      </TouchableOpacity>
      {showDatePicker && (
        <DateTimePicker
          value={date || new Date()}
          mode="date"
          display="default"
          onChange={(event, selectedDate) => {
            setShowDatePicker(false);
            if (selectedDate) {
              setDate(selectedDate);
            }
          }}
        />
      )}
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
  dateButton: {
    borderWidth: 1,
    borderColor: '#ccc',
    padding: 10,
    marginBottom: 10,
    justifyContent: 'center',
    alignItems: 'center',
  },
  radioContainer: {
    flexDirection: 'row',
    alignItems: 'center',
    marginBottom: 10,
  },
});

export default InventoryScreen;
