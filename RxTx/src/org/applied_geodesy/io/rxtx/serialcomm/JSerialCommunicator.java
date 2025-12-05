/***********************************************************************
* Copyright by Michael Loesler, https://software.applied-geodesy.org   *
*                                                                      *
* This program is free software; you can redistribute it and/or modify *
* it under the terms of the GNU General Public License as published by *
* the Free Software Foundation; either version 3 of the License, or    *
* at your option any later version.                                    *
*                                                                      *
* This program is distributed in the hope that it will be useful,      *
* but WITHOUT ANY WARRANTY; without even the implied warranty of       *
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
* GNU General Public License for more details.                         *
*                                                                      *
* You should have received a copy of the GNU General Public License    *
* along with this program; if not, see <http://www.gnu.org/licenses/>  *
* or write to the                                                      *
* Free Software Foundation, Inc.,                                      *
* 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.            *
*                                                                      *
***********************************************************************/

package org.applied_geodesy.io.rxtx.serialcomm;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.applied_geodesy.io.rxtx.ReceiveDataType;
import org.applied_geodesy.io.rxtx.RxTx;

import com.fazecast.jSerialComm.SerialPort;
import com.fazecast.jSerialComm.SerialPortDataListener;
import com.fazecast.jSerialComm.SerialPortEvent;


public class JSerialCommunicator extends RxTx implements SerialPortDataListener {
	private SerialPort   ubxPort;
	private InputStream  inputStream;
	private OutputStream outputStream;
	
	public JSerialCommunicator() {
		this.setBaudRate(9600);
		// Stopbit, Paritaet und Datenbits sind fest 1, None, 8
		this.setDataBits(8);
		this.setStopBits(SerialPort.ONE_STOP_BIT);
		this.setParity(SerialPort.NO_PARITY);
		this.setFlowControl(SerialPort.FLOW_CONTROL_DISABLED);
	}
	
	public SerialPort getSerialPort() {
		return ubxPort;
	}
	
	public void setSerialPort(SerialPort ubxPort) {
		this.ubxPort = ubxPort;
	}

	@Override
	public int getListeningEvents() {
		return SerialPort.LISTENING_EVENT_DATA_AVAILABLE;
	}

	@Override
	public synchronized void serialEvent(SerialPortEvent serialPortEvent) {
		if (serialPortEvent.getEventType() == SerialPort.LISTENING_EVENT_DATA_AVAILABLE) {
			byte[] readBuffer = new byte[0xFFFF];
			try {
				int readBytes = 0;
				while (this.inputStream.available() > 0) {
					if (this.getReceiveDataType() == ReceiveDataType.BYTE_ARRAY) {
						readBytes = this.inputStream.read(readBuffer);
						byte data[] = new byte[readBytes];
						System.arraycopy(readBuffer, 0, data, 0, readBytes);
						this.fireReceiveMessage(data);
					}
					else if (this.getReceiveDataType() == ReceiveDataType.INTEGER) {
						this.fireReceiveMessage(this.inputStream.read());
					}
				}
			} 
			catch (IOException e) {
				System.err.println("Fehler beim Datenempfang!");
				e.printStackTrace();
			}
		}	
	}

	@Override
	public void transmit(byte[] bytesTX) throws IOException {
		this.outputStream.write(bytesTX);
		this.outputStream.flush();
	}
	
	@Override
	public void transmit(int intTX) throws IOException {
		this.outputStream.write(intTX);
		this.outputStream.flush();
	}

	@Override
	public boolean open() {
		if (this.ubxPort == null)
			return false;

		this.ubxPort.setBaudRate(this.getBaudRate());
		this.ubxPort.setNumDataBits(this.getDataBits());
		this.ubxPort.setNumStopBits(this.getStopBits());
		this.ubxPort.setParity(this.getParity());
		this.ubxPort.setFlowControl(this.getFlowControl());
		this.ubxPort.setComPortTimeouts(SerialPort.TIMEOUT_NONBLOCKING, 0, 0);
		
		if (this.ubxPort.openPort()) {
//			this.ubxPort.setBaudRate(this.getBaudRate());
//			this.ubxPort.setNumDataBits(this.getDataBits());
//			this.ubxPort.setNumStopBits(this.getStopBits());
//			this.ubxPort.setParity(this.getParity());
//			this.ubxPort.setFlowControl(this.getFlowControl());
//			this.ubxPort.setComPortTimeouts(SerialPort.TIMEOUT_NONBLOCKING, 0, 0);
			
			this.inputStream  = this.ubxPort.getInputStream();
			this.outputStream = this.ubxPort.getOutputStream();
	
			this.ubxPort.removeDataListener();		
			this.ubxPort.addDataListener(this);
			
			return true;
		}
		
		return false;
	}

	@Override
	public boolean close() {
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		executorService.execute(new Runnable() {
			@Override
			public void run() {
				try {if (outputStream != null)	outputStream.close();} catch (IOException e) {e.printStackTrace();}
				try {if (inputStream != null)	inputStream.close(); } catch (IOException e) {e.printStackTrace();}
				try {
					if (ubxPort != null) {
						synchronized(this){
							ubxPort.removeDataListener();
							ubxPort.closePort();
						}
					}
				} catch (Exception e) {e.printStackTrace();}
			}
		});

		executorService.shutdown();
		try {
			executorService.awaitTermination(5, TimeUnit.SECONDS);
			if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
				executorService.shutdownNow(); // Cancel currently executing tasks
				if (!executorService.awaitTermination(5, TimeUnit.SECONDS))
					System.err.println(this.getClass().getSimpleName() + " PORT closing did not terminate!");
			}
		} catch (InterruptedException e) {
			executorService.shutdownNow(); // (Re-)Cancel if current thread also interrupted
			Thread.currentThread().interrupt(); // Preserve interrupt status
			return false;
		}
		return true;
	}
}
