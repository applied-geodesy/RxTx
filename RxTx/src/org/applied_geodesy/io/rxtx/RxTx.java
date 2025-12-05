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

package org.applied_geodesy.io.rxtx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class RxTx implements TransmitterExchangeable {
	private List<ReceiverExchangeable> receivers = new ArrayList<ReceiverExchangeable>();
	
	private int baudRate    = 9600;
	private int dataBits    = 8;
	private int stopBits    = 1;
	private int parity      = 0;
	private int flowControl = 0;
	private ReceiveDataType receiveDataType = ReceiveDataType.BYTE_ARRAY;
	
	public ReceiveDataType getReceiveDataType() {
		return this.receiveDataType;
	}
	
	public int getBaudRate() {
		return this.baudRate;
	}
	
	public int getDataBits() {
		return this.dataBits;
	}
	
	public int getStopBits() {
		return this.stopBits;
	}
	
	public int getParity() {
		return this.parity;
	}
	
	public int getFlowControl() {
		return this.flowControl;
	}
	
	public void setReceiveDataType(ReceiveDataType receiveDataType) {
		this.receiveDataType = receiveDataType;
	}
	
	public void setBaudRate(int baudRate) throws IllegalArgumentException {
		if (baudRate % 1200 != 0)
			throw new IllegalArgumentException("Error, invalid baud rate " + baudRate);
		this.baudRate = baudRate;
	}
	
	public void setDataBits(int dataBits) throws IllegalArgumentException {
		this.dataBits = dataBits;
	}
	
	public void setStopBits(int stopBits) throws IllegalArgumentException {
		this.stopBits = stopBits;
	}
	
	public void setParity(int parity) throws IllegalArgumentException {
		this.parity = parity;
	}
	
	public void setFlowControl(int flowControl) throws IllegalArgumentException {
		this.flowControl = flowControl;
	}
	
	public void addReceiver(ReceiverExchangeable receiver) {
		this.receivers.add(receiver);
	}
	
	public void removeReceiver(ReceiverExchangeable receiver) {
		this.receivers.remove(receiver);
	}
	
	public void fireReceiveMessage(byte[] bytesRX) throws IOException {
		for (ReceiverExchangeable receiver : this.receivers)
			receiver.receive(bytesRX);
	}
	
	public void fireReceiveMessage(int intRX) throws IOException {
		for (ReceiverExchangeable receiver : this.receivers)
			receiver.receive(intRX);
	}
	
	public void transmit(String msgTX) throws IOException {
		this.transmit(msgTX.getBytes());
	}
	
	public void transmit(byte byteTX) throws IOException {
		this.transmit(new byte[] {byteTX});
	}
	
	public abstract boolean open();
	
	public abstract boolean close();
}
