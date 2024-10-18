import React, { useState, useEffect } from 'react';
import { View, Text, Button, StyleSheet, TouchableOpacity } from 'react-native';
import { CameraView, CameraType, useCameraPermissions } from 'expo-camera';
import { RootStackParamList } from './types';
import useProductStore from '../store/useAppStore'; // Importar useAppStore
import { StackNavigationProp } from '@react-navigation/stack';
import { Audio } from 'expo-av';

type NewInventoryNavigationProp = StackNavigationProp<RootStackParamList, 'NewInventory' | 'Capture'>;
type Props = { navigation: NewInventoryNavigationProp };

const CaptureScreen: React.FC<Props> = ({ navigation }) => {
  const [scanned, setScanned] = useState(false);  // Estado para evitar múltiples lecturas
  const [facing, setFacing] = useState<CameraType>('back');
  const [permission, requestPermission] = useCameraPermissions();
  const { addProduct } = useProductStore();
  const [sound, setSound] = useState(null); // Sonido al escanear
  const [product, setProduct] = useState({ code: '', title: '', quantity: 0, unit: '' });

  useEffect(() => {
    requestPermission();
  }, []);

  useEffect(() => {
    return sound ? () => sound.unloadAsync() : undefined;
  }, [sound]);

  const getProduct = async (code: any) => {
    if (!code) return false;

    const request = await fetch(`https://api.bsale.cl/v1/variants.json?code=${code}&expand=[products]`, {
      method: 'GET',
      headers: { 'access_token': 'e57d148002d91e58f152bece58d810e50bc84286' }
    });

    const response = await request.json();
    return response || false;
  };

  const playSound = async () => {
    const { sound } = await Audio.Sound.createAsync(require('../assets/audio/coin.mp3'));
    setSound(sound);
    await sound.playAsync();
  };

  const onReadQr = async (event: any) => {
    if (scanned) return;  // Si ya se escaneó, no hacer nada
    setScanned(true);  // Bloquear la lectura mientras se procesa

    const data = event.data;
    if (data) {
      playSound();

      const request = await getProduct(data);
      const productName = request?.items[0]?.product?.name || 'Producto desconocido';
      const productCode = data;

      addProduct({ code: productCode, title: productName, quantity: 0, unit: '' });
      setProduct({ code: '', title: '', quantity: 0, unit: '' });

      navigation.navigate('NewInventory');
      setScanned(false);  // Permitir escanear nuevamente

      console.log (data)
    }
  };

  if (!permission) return <View />;
  if (!permission.granted) {
    return (
      <View>
        <Text>We need your permission to show the camera</Text>
        <Button onPress={requestPermission} title="Grant permission" />
      </View>
    );
  }

  const toggleCameraFacing = () => setFacing((current) => (current === 'back' ? 'front' : 'back'));

  return (
    <View style={styles.container}>
      <CameraView
        style={styles.camera}
        facing={facing}
        barcodeScannerSettings={{ barcodeTypes: ["qr"] }}
        onBarcodeScanned={scanned ? undefined : onReadQr}  // Solo se escanea si no ha sido escaneado
      >
        <View style={styles.buttonContainer}>
          <TouchableOpacity style={styles.button} onPress={toggleCameraFacing}>
            <Text style={styles.text}>Flip Camera</Text>
          </TouchableOpacity>
        </View>
      </CameraView>
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
  },
  camera: {
    flex: 0.2,
    padding: 80,
  },
  buttonContainer: {
    flex: 1,
    flexDirection: 'row',
    backgroundColor: 'transparent',
    margin: 64,
  },
  button: {
    flex: 1,
    alignSelf: 'flex-end',
    alignItems: 'center',
  },
  text: {
    fontSize: 24,
    fontWeight: 'bold',
    color: 'white',
  },
});

export default CaptureScreen;
