// Archivo: CaptureScreen.tsx
import React from 'react';
import { View, Text, Button, StyleSheet } from 'react-native';
import { BarCodeScanner } from 'expo-barcode-scanner';
import { RouteProp } from '@react-navigation/native';
import { RootStackParamList } from './types';

type CaptureScreenRouteProp = RouteProp<RootStackParamList, 'Capture'>;

type Props = {
  route: CaptureScreenRouteProp;
};

const CaptureScreen: React.FC<Props> = ({ route }) => {
  const { warehouse, date } = route.params;

  const handleBarCodeScanned = ({ type, data }: { type: string; data: string }) => {
    alert(`Código QR escaneado: ${data}`);
    // Aquí podrías guardar el dato escaneado
  };

  return (
    <View style={styles.container}>
      <Text style={styles.title}>Inventario {date}</Text>
      <Text>{warehouse}</Text>
      <BarCodeScanner
        onBarCodeScanned={handleBarCodeScanned}
        style={{ height: 300, width: 300 }}
      />
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
});

export default CaptureScreen;
