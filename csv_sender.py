import yagmail
import random
import time
import threading
import queue
import logging
import signal
import sys
import uuid
import fitz
import os

# PDF containing the contacts
PDF_FILE = "book1.pdf"

# Fetch credentials from environment variables
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")

if not EMAIL_USER or not EMAIL_PASS:
    print("Error: EMAIL_USER or EMAIL_PASS environment variables not set.")
    sys.exit(1)

sender_accounts = [
    {"email": EMAIL_USER, "app_password": EMAIL_PASS}
]

linkedin_url = "https://www.linkedin.com/in/abhaypadavi01"
TRACKING_SERVER = "http://127.0.0.1:5000"  # Change to your deployed server URL on Render
drive_link = "https://drive.google.com/file/d/1bemImkvQ62BP3glYnm8b88p9jcQsTDG0/view?usp=drive_link"

subject_template = "Application for Technical Jobs - Passionate Software Engineer Seeking Opportunity"

body_template = """<html><body>
Dear {name},<br>
I hope this message finds you well.<br>
My name is Abhay Padavi, and I’m reaching out with genuine interest in exploring any opportunities within your esteemed organization. I am passionate about contributing meaningfully to a forward-thinking team like yours and eager to bring my energy, curiosity, and growing skill set to a dynamic work environment.
I’ve developed a strong foundation in PL/SQL, Python, SQL, HTML, CSS, ASP.NET, C#, and Linux, and I’ve applied these through hands-on job simulations offered by Forage, gaining valuable exposure to real-world business scenarios.
Currently, I’m working as an intern at Hindustan Aeronautics Limited, where I'm contributing to backend Oracle databases — maintaining data, building dashboards, and optimizing performance to handle client requests more efficiently.
I’d love the opportunity to connect and learn more about your team. Would you be open to a quick chat to explore any opportunities that might align with my background?
I’ve attached my resume link here for your reference: <a href="{tracked_link}">Click here to view my resume</a>
Thank you so much for your time — I’d truly appreciate any insights or guidance you can share.
Looking forward to hearing from you!</br>
Warm regards,
Abhay Padavi
<a href="{linkedin_url}">LinkedIn</a>
<br>+91-9558538691<br>{email}<br>
<img src="{open_pixel_url}" alt="" width="1" height="1" style="display:none;">
</body></html>"""

NUM_THREADS = 2
MIN_DELAY = 5
MAX_DELAY = 15
MAX_RETRIES = 3
BATCH_SIZE = 50

email_queue = queue.Queue()
stop_event = threading.Event()
lock = threading.Lock()
stats = {"sent": 0, "failed": 0, "retries": 0}

def generate_tracking_id():
    return str(uuid.uuid4())

def extract_contacts_from_pdf(pdf_path):
    contacts = {}
    doc = fitz.open(pdf_path)
    for page in doc:
        text = page.get_text()
        lines = text.splitlines()
        for line in lines:
            if "@" in line:
                parts = line.split()
                for part in parts:
                    if "@" in part:
                        email = part.strip(",")
                        name = line.replace(email, "").strip()
                        contacts[email] = name
                        break
    return contacts

def send_email(from_account, to_email, name):
    attempt = 0
    while attempt <= MAX_RETRIES and not stop_event.is_set():
        attempt += 1
        yag = None
        try:
            yag = yagmail.SMTP(user=from_account["email"], password=from_account["app_password"])
            tracking_id = generate_tracking_id()

            open_pixel_url = f"{TRACKING_SERVER}/pixel?id={tracking_id}&email={to_email}"
            tracked_link = f"https://auto-reach-radar.onrender.com/redirect?id={tracking_id}&email={to_email}&url={drive_link}"

            email_body_html = body_template.format(
                name=name,
                email=from_account["email"],
                open_pixel_url=open_pixel_url,
                tracked_link=tracked_link,
                drive_link=drive_link,
                linkedin_url=linkedin_url
            )

            yag.send(
                to=to_email,
                subject=subject_template,
                contents=email_body_html,
                headers={"Reply-To": from_account["email"]}
            )

            logging.info(f"Email sent to {to_email} (Tracking ID: {tracking_id})")
            with lock:
                stats["sent"] += 1
                if attempt > 1:
                    stats["retries"] += (attempt - 1)
            return True
        except Exception as e:
            logging.error(f"Error sending to {to_email}: {e}")
            if attempt >= MAX_RETRIES:
                with lock:
                    stats["failed"] += 1
            time.sleep(5 * attempt)
        finally:
            if yag:
                yag.close()

def worker_thread(thread_id):
    while not stop_event.is_set():
        try:
            from_account, recipient, name = email_queue.get(timeout=5)
        except queue.Empty:
            break
        send_email(from_account, recipient, name)
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        email_queue.task_done()

def enqueue_batch(hr_dict, batch_size):
    items = list(hr_dict.items())[:batch_size]
    for i, (recipient, name) in enumerate(items):
        from_account = sender_accounts[i % len(sender_accounts)]
        email_queue.put((from_account, recipient, name))

def signal_handler(sig, frame):
    logging.warning("Stopping program...")
    stop_event.set()

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if not os.path.exists(PDF_FILE):
        print(f" PDF file '{PDF_FILE}' not found.")
        return

    hr_contacts = extract_contacts_from_pdf(PDF_FILE)
    if not hr_contacts:
        print("No valid email entries found in PDF.")
        return

    stats["total"] = len(hr_contacts)
    enqueue_batch(hr_contacts, BATCH_SIZE)

    threads = []
    for i in range(NUM_THREADS):
        t = threading.Thread(target=worker_thread, args=(i,), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    email_queue.join()
    logging.info(f"Finished. Sent: {stats['sent']}, Failed: {stats['failed']}")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler("email_sender.log"), logging.StreamHandler(sys.stdout)]
    )

    main()
