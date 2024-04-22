import { Test, TestingModule } from '@nestjs/testing';
import { MdrService } from './mdr.service';

describe('MdrService', () => {
  let service: MdrService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [MdrService],
    }).compile();

    service = module.get<MdrService>(MdrService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
