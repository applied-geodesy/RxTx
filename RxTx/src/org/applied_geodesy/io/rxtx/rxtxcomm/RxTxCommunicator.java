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

package org.applied_geodesy.io.rxtx.rxtxcomm;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.TooManyListenersException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.applied_geodesy.io.rxtx.ReceiveDataType;
import org.applied_geodesy.io.rxtx.RxTx;

import gnu.io.CommPort;
import gnu.io.CommPortIdentifier;
import gnu.io.PortInUseException;
import gnu.io.SerialPort;
import gnu.io.SerialPortEvent;
import gnu.io.SerialPortEventListener;
import gnu.io.UnsupportedCommOperationException;

public class RxTxCommunicator extends RxTx implements SerialPortEventListener {
	private CommPortIdentifier portId;
	private SerialPort serialPort;
	
	private InputStream  inputStream;
	private OutputStream outputStream;
	
	public RxTxCommunicator() {
		this.setDataBits(SerialPort.DATABITS_8);
		this.setStopBits(SerialPort.STOPBITS_1);
		this.setParity(SerialPort.PARITY_NONE);
		this.setBaudRate(9600); //4800; //9600;
	}

	@Override
	public synchronized void serialEvent(SerialPortEvent event) {	 
		if (event.getEventType() == SerialPortEvent.DATA_AVAILABLE) {
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
	

	public CommPortIdentifier getCommPortIdentifier() {
		return portId;
	}
	
	public void setCommPortIdentifier(CommPortIdentifier portId) {
		this.portId = portId;
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
		try {
			if (this.portId == null)
				return false;
			
			if (this.portId.isCurrentlyOwned() ) {
				System.err.println(this.getClass().getSimpleName() + " Fehler, PORT " + this.portId.getName() + " wird bereits verwendet von " + this.portId.getCurrentOwner() + "!");
				return false;
			}

			CommPort commPort = this.portId.open(this.getClass().getSimpleName(), 2000);
			if (commPort instanceof SerialPort) {
				this.serialPort = (SerialPort)commPort;

				this.serialPort.setSerialPortParams(
						this.getBaudRate(), 
						this.getDataBits(), 
						this.getStopBits(), 
						this.getParity()
				);

				this.inputStream  = this.serialPort.getInputStream();
				this.outputStream = this.serialPort.getOutputStream();

				this.serialPort.removeEventListener();
				this.serialPort.addEventListener(this);
				this.serialPort.notifyOnDataAvailable(true);
			}
			else {
				System.err.println(this.getClass().getSimpleName() + " Fehler, PORT " + this.portId.getName() + " ist kein SerialPort!");
				return false;
			}
		} catch (PortInUseException e) {
			e.printStackTrace();
			return false;
		} catch (UnsupportedCommOperationException e) {
			e.printStackTrace();
			return false;
		} catch (TooManyListenersException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
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
					if (serialPort != null) {
						synchronized(this){
							serialPort.removeEventListener();
							serialPort.addEventListener(null);				
							serialPort.close();
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
