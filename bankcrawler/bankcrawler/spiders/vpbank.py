import scrapy
import os
from urllib.parse import urljoin

class VpbankPDFSpider(scrapy.Spider):
   name = "vpbank_pdf_only"
   allowed_domains = ["vpbank.com.vn"]
   start_urls = ["https://www.vpbank.com.vn/ca-nhan/the-tin-dung"]

   def __init__(self):
      self.output_dir = "pdf"
      os.makedirs(self.output_dir, exist_ok=True)
      self.visited_urls = set()

   def parse(self, response):
      if response.url in self.visited_urls:
         return
      self.visited_urls.add(response.url)
      
      # Từ khóa liên quan đến tín dụng
      keywords = ["tin-dung", "tindung", "credit", "visa"]

      # Duyệt các liên kết trên trang
      for href in response.css("a::attr(href)").getall():
         href_lower = href.lower()
         full_url = urljoin(response.url, href)

         # Bỏ qua biểu mẫu không cần
         if "bieu-mau" in href_lower or "bieumau" in href_lower:
               continue

         # Nếu là file PDF và có chứa từ khóa liên quan đến tín dụng
         if href_lower.endswith(".pdf") and any(kw in href_lower for kw in keywords):
               yield scrapy.Request(full_url, callback=self.save_pdf)

         # Nếu là trang HTML, tiếp tục crawl
         elif href_lower.startswith("http") or href.startswith("/"):
               yield scrapy.Request(full_url, callback=self.parse)

   def save_pdf(self, response):
      pdf_name = response.url.split("/")[-1].split("?")[0]
      if not pdf_name.endswith(".pdf"):
         return  # Đảm bảo chỉ lưu file .pdf

      path = os.path.join(self.output_dir, pdf_name)
      with open(path, "wb") as f:
         f.write(response.body)
      self.logger.info(f"✅ Downloaded PDF: {pdf_name}")
