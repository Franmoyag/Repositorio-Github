//Screen Principal

import React from 'react';
import { View, Text, TouchableOpacity, StyleSheet } from 'react-native';
import { StackNavigationProp } from '@react-navigation/stack';
import { RootStackParamList } from './types';
import { useAppStore } from '../store/useAppStore';

type HomeScreenNavigationProp = StackNavigationProp<RootStackParamList, 'Home'>;

type Props = {
  navigation: HomeScreenNavigationProp;
};

const HomeScreen: React.FC<Props> = ({ navigation }) => {

  const inventories = useAppStore((state) => state.inventories);

  return (
    <View style={styles.container}>
      <Text style={styles.title}>Inventarios Kamver</Text>
      <TouchableOpacity style={styles.button} onPress={() => navigation.navigate('Inventory')}>
        <Text style={styles.buttonText}>Iniciar Inventario</Text>
      </TouchableOpacity>
      <TouchableOpacity style={[styles.button, styles.marginTop]} onPress={() => navigation.navigate('InventoriesList')}>
  <Text style={styles.buttonText}>Ver Inventarios</Text>
</TouchableOpacity>
      {inventories.length === 0 && (
        <Text style={styles.noInventoriesText}>Aun no hay inventarios realizados</Text>
      )}
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
  },
  title: {
    fontSize: 24,
    marginBottom: 20,
  },
  button: {
    backgroundColor: '#007BFF',
    padding: 10,
    borderRadius: 5,
    alignItems: 'center',
    width: 200,
  },
  buttonText: {
    color: '#FFFFFF',
    fontWeight: 'bold',
  },
  marginTop: {
    marginTop: 10,
  },
  noInventoriesText: {
    marginTop: 20,
    fontSize: 16,
    color: '#666',
  },
});

export default HomeScreen;
