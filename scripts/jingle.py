import numpy as np
import wave
import struct
import os

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
BASE_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
SOUND_DIR = os.path.join(BASE_DIR, "Extra", "sounds")

def generate_success_jingle(filename):
    sample_rate = 44100
    duration = 0.15  # seconds per note
    frequencies = [523, 659, 783, 1046]  # C5, E5, G5, C6 (major arpeggio)
    amplitude = 8000
    audio = []

    for freq in frequencies:
        for i in range(int(sample_rate * duration)):
            sample = amplitude * np.sin(2 * np.pi * freq * i / sample_rate)
            audio.append(int(sample))

    # Add a tiny silence at the end
    silence = [0] * int(sample_rate * 0.1)
    audio.extend(silence)

    # Write to WAV
    wav_file = wave.open(filename, 'w')
    wav_file.setparams((1, 2, sample_rate, 0, 'NONE', 'not compressed'))

    for sample in audio:
        wav_file.writeframes(struct.pack('<h', sample))

    wav_file.close()
    print(f"WAV file saved as {filename}")

filename = os.path.join(SOUND_DIR,"success_jingle.wav")
generate_success_jingle(filename)
