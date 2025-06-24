import tkinter as tk
from tkinter import filedialog
from tkinter import ttk

import os
from PIL import Image, ImageTk

from openai import OpenAI
from openai import AuthenticationError

import ollama

import base64

# Replace with your OpenAI API key
OPENAI_API_KEY = "your_openai_api_key"

class ImageBrowser:
    def __init__(self, root):
        self.root = root
        self.root.title("Image Browser")
        self.current_selected_image = None

        # Left Frame - Listbox for filenames
        self.frame_left = tk.Frame(self.root)
        self.frame_left.pack(side=tk.LEFT, fill=tk.Y)

        self.listbox = tk.Listbox(self.frame_left)
        self.listbox.pack(fill=tk.BOTH, expand=True)
        self.listbox.bind("<<ListboxSelect>>", self.load_image)

        # Button to open file dialog
        self.open_button = tk.Button(self.root, text="Choose Directory", command=self.choose_directory)
        self.open_button.pack()

        # Button to runt the AI analysis
        self.run_button = tk.Button(self.root, text="Run AI Analysis", command=self.get_ai_rating)
        self.run_button.pack()

        # Center Canvas for image display
        self.canvas = tk.Canvas(self.root, width=400, height=400)
        self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Right Frame - Listbox for rating categories
        self.frame_right = tk.Frame(self.root)
        self.frame_right.pack(side=tk.RIGHT, fill=tk.Y)

        # Textbox for AI rating display with scrollbar
        self.rating_frame = tk.Frame(self.frame_right)
        self.rating_frame.pack(fill=tk.BOTH, expand=True)

        self.rating_text = tk.Text(self.rating_frame, height=40, width=50, wrap="word", yscrollcommand=lambda *args: self.scrollbar.set(*args))
        self.rating_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.scrollbar = tk.Scrollbar(self.rating_frame, command=self.rating_text.yview)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.model_options = ["Local Model", "Open AI model"]
        self.model_choice = tk.StringVar(value=self.model_options[0])
        self.model_choice.trace_add("write", self.on_selection_change)
        ttk.Label(root, text="Choose your model:").pack(pady=10)

        # Option menu
        option_menu = ttk.OptionMenu(root, self.model_choice, self.model_options[0], *self.model_options)
        option_menu.pack()

        self.image_list = []
        self.image_dir = ""
        self.img_path = ""

    def choose_directory(self):
        """Open file dialog and list .jpg files"""
        self.image_dir = filedialog.askdirectory()
        if self.image_dir:
            self.image_list = [f for f in os.listdir(self.image_dir) if f.lower().endswith(".jpg")]
            self.listbox.delete(0, tk.END)
            for img in self.image_list:
                self.listbox.insert(tk.END, img)

    def load_image(self, event):
        """Load selected image onto canvas"""
        selected_index = self.listbox.curselection()
        if selected_index:
            self.img_path = os.path.join(self.image_dir, self.image_list[selected_index[0]])
            self.current_selected_image = self.img_path
            image = Image.open(self.img_path)
            image = image.resize((400, 400), Image.Resampling.LANCZOS)
            self.img = ImageTk.PhotoImage(image)

            self.canvas.delete("all")
            self.canvas.create_image(200, 200, image=self.img)

            self.rating_text.delete("1.0", tk.END)

    def get_ai_rating(self):
        """Get the selected index and then call the function that gets the ai help"""
        # Show message box while waiting for response
        wait_window = tk.Toplevel(self.root)
        wait_window.title("Processing...")
        wait_label = tk.Label(wait_window, text="AI is evaluating the image...\nPlease wait.", padx=20, pady=10)
        wait_label.pack()
        wait_window.update()

        if self.model_choice.get() == "Open AI model":
            self.get_ai_rating_openai(wait_window)

        if self.model_choice.get() == "Local Model":
            self.get_ai_rating_local(image=self.img_path, wait_window=wait_window)

    def get_ai_rating_openai(self, wait_window):
        """Get OpenAI rating for the selected criterion"""
        # Request OpenAI's evaluation 
        try:
            
            self.query_openai("You are an expert image evaluator for an online shopping site. Rate the image from 1 to 10", wait_window)

        except AuthenticationError:
            wait_window.destroy()
            self.rating_text.insert('end', "You will need to add your OpenAI key in order to use this model.")
        
    def get_ai_rating_local(self, image, wait_window):
        """Get local model rating for the selected criterion"""
        stream = ollama.chat(
            model = "llava:7b",
            stream = True,
            messages=[
                {
                    'role': 'user',
                    'content': f"You are an expert image evaluator for on online shopping site. " 
                    "Rate the image from 1 to 10.  Keep the response to only 100 words or less.",
                    'images': [image]
                }
            ] 
        )
        for chunk in stream:
            if wait_window:
                wait_window.destroy()

            self.add_text(chunk.message.content)
        
    def add_text(self, added_text):
        self.rating_text.insert('end', added_text)
        self.rating_text.see('end')  # Scroll to the end if needed
        self.rating_text.update_idletasks()

    def query_openai(self, prompt, wait_window):
        with open(self.current_selected_image, "rb") as image_file:
            b64_image = base64.b64encode(image_file.read()).decode("utf-8")
        
        client = OpenAI(api_key=OPENAI_API_KEY,)
        response = client.responses.create(
            model="gpt-4o-mini",
            input=[
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": prompt},
                        {"type": "input_image", "image_url": f"data:image/png;base64,{b64_image}"},
                    ],
                }
            ],
            stream=True 
        )
        for chunk in response:
            if wait_window:
                wait_window.destroy()

            if hasattr(chunk, "delta"):
                self.add_text(chunk.delta)

    def on_selection_change(self, *args):
        print("Selected model:", self.model_choice.get())
        self.rating_text.delete("1.0", tk.END)

if __name__ == "__main__":
    root = tk.Tk()
    app = ImageBrowser(root)
    root.mainloop()