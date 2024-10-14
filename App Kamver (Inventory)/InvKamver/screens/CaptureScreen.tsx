import { View, Text } from "react-native";
import React from "react";
import QRCodeScanner from "react-native-qrcode-scanner"
import { RNCamera } from "react-native-camera";

const CaptureScreen = () => {
  return (
    <QRCodeScanner
      onRead={({data}) => alert(data)}
      reactivate={true}
      reactivateTimeout={500}
      topContent={
        <View>
          <Text>Scanner</Text>
        </View>
      }
    />
  );
}

export default CaptureScreen