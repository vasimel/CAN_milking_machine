import tkinter as tk
from tkinter.scrolledtext import ScrolledText
import can_script
import threading

def run_query():
    # Запуск can_script.main в отдельном потоке
    threading.Thread(target=lambda: can_script.main(output_func=custom_print), daemon=True).start()

def custom_print(text):
    # Вставка текста в ScrolledText виджет и автоматическая прокрутка к последней записи
    # Так как эта функция вызывается из другого потока, нужно использовать метод after для взаимодействия с GUI
    def insert_text():
        output_text.config(state=tk.NORMAL)  # Разрешить вставку текста
        output_text.insert(tk.END, text + "\n")  # Вставить текст
        output_text.see(tk.END)  # Прокрутка к последней записи
        output_text.config(state=tk.DISABLED)  # Запретить редактирование
    output_text.after(0, insert_text)

root = tk.Tk()
root.title("Опрос доильных аппаратов")

button = tk.Button(root, text="Запуск опроса", command=run_query)
button.pack()

# Создание ScrolledText виджета для вывода
output_text = ScrolledText(root, height=10, state=tk.DISABLED)
output_text.pack()

root.mainloop()
