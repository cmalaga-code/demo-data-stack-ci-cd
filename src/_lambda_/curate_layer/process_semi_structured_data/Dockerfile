FROM public.ecr.aws/lambda/python:3.12
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY semi_structured.py .
CMD ["semi_structured.handler"]
