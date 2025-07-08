FROM public.ecr.aws/lambda/python:3.12
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY unstructured.py .
CMD ["unstructured.handler"]
