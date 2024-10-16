import React from 'react';
import { StackNavigationProp } from '@react-navigation/stack';
import { RootStackParamList } from './types';
import { StyleSheet, View } from 'react-native';
import { FAB } from 'react-native-paper'
import Icon from 'react-native-vector-icons/Ionicons';

type NewInventoryNavigationProp = StackNavigationProp<RootStackParamList, 'NewInventory'| 'Capture'>;

type Props = {navigation: NewInventoryNavigationProp;};




const NewInventory: React.FC<Props> = ({ navigation }) => {
  return (
    <View style={styles.container}>
      <FAB
        icon={(props) => <Icon {...props} name="camera-outline" size={25} />}
        label="Nueva Captura"
        style={styles.fab}
        onPress={() => navigation.navigate('Capture')} // Navegar a CaptureScreen
      />
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1, // Para que el contenedor ocupe toda la pantalla
    justifyContent: 'center', // Centra el contenido verticalmente
    alignItems: 'center', // Centra el contenido horizontalmente
  },
  fab: {
    position: 'absolute',
    margin: 16,
    right: 0,
    bottom: 20,
  },
});

export default NewInventory;