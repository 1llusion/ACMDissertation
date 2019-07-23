import matplotlib.style as ms
import librosa.display
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path


class Spectrogram:
    # https://nbviewer.jupyter.org/github/librosa/librosa/blob/master/examples/LibROSA%20demo.ipynb
    #ms.use('seaborn-muted')

    @staticmethod
    def get_spectrogram(audio_path, output = Path()):
        y, sr = librosa.load(audio_path, sr=None)
        s = librosa.feature.melspectrogram(y, sr=sr, n_mels=128)
        log_s = librosa.power_to_db(s, ref=np.max)

        plt.figure(figsize=(24, 8))

        librosa.display.specshow(log_s, sr=sr, x_axis='time', y_axis='mel')
        plt.axis("off")

        plt.tight_layout()
        plt.savefig(str(output))
        plt.close('all')
