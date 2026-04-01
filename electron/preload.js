const { contextBridge, ipcRenderer } = require('electron');

// Expose protected methods that allow the renderer process to use
// specific IPC methods without exposing the entire API
contextBridge.exposeInMainWorld('electronAPI', {
  quitApp: () => ipcRenderer.send('quit-app'),
  platform: process.platform,
  isPackaged: process.env.NODE_ENV === 'production'
});